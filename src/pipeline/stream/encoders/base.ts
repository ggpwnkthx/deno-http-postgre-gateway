import type { PerfCtx } from "../../../shared/perf.ts";

// Single shared encoder avoids per-stream allocations.
const ENCODER = new TextEncoder();

/** Exported base64 helpers for reuse (e.g., CSV). Chunked to avoid arg limits. */
export function u8ToBase64(u8: Uint8Array): string {
  const CHUNK = 0x8000;
  const parts: string[] = [];
  for (let i = 0; i < u8.length; i += CHUNK) {
    const slice = u8.subarray(i, i + CHUNK);
    // Array.from creates a small copy per chunk to satisfy apply(...args)
    parts.push(String.fromCharCode.apply(null, Array.from(slice) as unknown as number[]));
  }
  return `base64:${btoa(parts.join(""))}`;
}
export function abToBase64(ab: ArrayBuffer): string {
  return u8ToBase64(new Uint8Array(ab));
}

// -------------------- Line-buffered stream (kept for compatibility) --------------------
class LineBuffered {
  private closed = false;
  private timer: number | null = null;
  private readonly immediate: boolean;

  private chunksStr: string[] = [];

  // perf
  private bytesOut = 0;
  private linesOut = 0;
  private flushes = 0;

  constructor(
    private controller: ReadableStreamDefaultController<Uint8Array>,
    private flushEvery: number,
    private intervalMs: number,
    onStart: ((push: (s: string) => void) => void) | undefined,
    private onCancel: (() => void) | undefined,
    private perf: PerfCtx | undefined,
  ) {
    this.immediate = this.flushEvery === 0;
    if (this.intervalMs > 0) {
      this.timer = setInterval(() => this.flush(), this.intervalMs) as unknown as number;
    }
    if (onStart) onStart((s) => this.pushLine(s));
  }

  private enqueueBytes(u8: Uint8Array) {
    try {
      this.controller.enqueue(u8);
    } catch {
      // ignore enqueue after close
    }
  }

  pushLine(line: string) {
    if (this.closed) return;

    if (this.immediate) {
      const u8 = ENCODER.encode(line);
      this.enqueueBytes(u8);
      this.bytesOut += u8.byteLength;
      this.linesOut++;
      return;
    }

    this.chunksStr.push(line);
    this.linesOut++;
    if (this.flushEvery > 0 && this.chunksStr.length >= this.flushEvery) {
      this.flush();
    }
  }

  flush() {
    if (this.closed || this.immediate) return;
    if (this.chunksStr.length === 0) return;

    const span = this.perf?.span("stream.flush", { chunks: this.chunksStr.length });
    const joined = this.chunksStr.join("");
    this.chunksStr.length = 0;
    const u8 = ENCODER.encode(joined);
    this.enqueueBytes(u8);
    this.bytesOut += u8.byteLength;
    this.flushes++;
    span?.mark("bytes", u8.byteLength);
    span?.end();
  }

  close() {
    if (this.closed) return;
    this.flush();
    if (this.timer !== null) clearInterval(this.timer);
    this.closed = true;
    try {
      this.controller.close();
    } catch {
      // ignore
    } finally {
      const span = this.perf?.span("stream", {});
      span?.mark("bytesOut", this.bytesOut);
      span?.mark("linesOut", this.linesOut);
      span?.mark("flushes", this.flushes);
      span?.end();
    }
  }

  cancel() {
    if (this.closed) return;
    this.close();
    try {
      this.onCancel?.();
    } catch {
      // ignore
    }
  }
}

// -------------------- Chunk-buffered stream (supports onStart) --------------------
class ChunkBuffered {
  private closed = false;
  private timer: number | null = null;
  private readonly immediate: boolean;

  private chunksU8: Uint8Array[] = [];
  private totalBytes = 0;

  // perf
  private bytesOut = 0;
  private flushes = 0;

  constructor(
    private controller: ReadableStreamDefaultController<Uint8Array>,
    private flushEvery: number,
    private intervalMs: number,
    onStart: ((push: (u8: Uint8Array) => void) => void) | undefined,
    private onCancel: (() => void) | undefined,
    private perf: PerfCtx | undefined,
  ) {
    this.immediate = this.flushEvery === 0;
    if (this.intervalMs > 0) {
      this.timer = setInterval(() => this.flush(), this.intervalMs) as unknown as number;
    }
    if (onStart) onStart((u8) => this.pushBytes(u8)); // initial bytes without copies
  }

  private enqueueBytes(u8: Uint8Array) {
    try {
      this.controller.enqueue(u8);
    } catch {
      // ignore enqueue after close
    }
  }

  pushBytes(u8: Uint8Array) {
    if (this.closed) return;

    if (this.immediate) {
      this.enqueueBytes(u8);
      this.bytesOut += u8.byteLength;
      return;
    }

    this.chunksU8.push(u8);
    this.totalBytes += u8.byteLength;
    if (this.flushEvery > 0 && this.chunksU8.length >= this.flushEvery) {
      this.flush();
    }
  }

  flush() {
    if (this.closed || this.immediate) return;
    if (this.chunksU8.length === 0) return;

    const span = this.perf?.span("stream.flush", {
      chunks: this.chunksU8.length,
      bytes: this.totalBytes,
    });

    const out = new Uint8Array(this.totalBytes);
    let off = 0;
    for (let i = 0; i < this.chunksU8.length; i++) {
      const c = this.chunksU8[i];
      out.set(c, off);
      off += c.byteLength;
    }
    this.chunksU8 = [];
    this.totalBytes = 0;

    this.enqueueBytes(out);
    this.bytesOut += out.byteLength;
    this.flushes++;
    span?.end();
  }

  close() {
    if (this.closed) return;
    this.flush();
    if (this.timer !== null) clearInterval(this.timer);
    this.closed = true;
    try {
      this.controller.close();
    } catch {
      // ignore
    } finally {
      const span = this.perf?.span("stream", {});
      span?.mark("bytesOut", this.bytesOut);
      span?.mark("flushes", this.flushes);
      span?.end();
    }
  }

  cancel() {
    if (this.closed) return;
    this.close();
    try {
      this.onCancel?.();
    } catch {
      // ignore
    }
  }
}

// -------------------- Factories --------------------
export function createLineStream(
  flushEvery: number,
  flushIntervalMs: number,
  onStart?: (push: (s: string) => void) => void,
  onCancel?: () => void,
  perf?: PerfCtx,
) {
  let impl: LineBuffered;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      impl = new LineBuffered(
        controller as ReadableStreamDefaultController<Uint8Array>,
        flushEvery,
        flushIntervalMs,
        onStart,
        onCancel,
        perf,
      );
    },
    cancel() {
      impl.cancel();
    },
  });
  return {
    stream,
    push: (s: string) => impl.pushLine(s),
    close: () => impl.close(),
  };
}

/**
 * Chunk (byte) stream with optional onStart hook for initial framing bytes.
 * Zero-copy path is achieved by using `flushEvery=0` (default in your app),
 * which enqueues each chunk directly without concatenation/joins.
 */
export function createChunkStream(
  flushEvery: number,
  flushIntervalMs: number,
  onStart?: (push: (u8: Uint8Array) => void) => void,
  onCancel?: () => void,
  perf?: PerfCtx,
) {
  let impl: ChunkBuffered;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      impl = new ChunkBuffered(
        controller as ReadableStreamDefaultController<Uint8Array>,
        flushEvery,
        flushIntervalMs,
        onStart,
        onCancel,
        perf,
      );
    },
    cancel() {
      impl.cancel();
    },
  });
  return {
    stream,
    pushBytes: (u8: Uint8Array) => impl.pushBytes(u8),
    close: () => impl.close(),
  };
}

// Keep jsonReplacer here for callers that need it.
export function jsonReplacer(_key: string, value: unknown) {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return u8ToBase64(value);
  if (value instanceof ArrayBuffer) return abToBase64(value);
  return value as unknown;
}

// Small helper for encoders that want to pre-encode small tokens.
export const encodeUtf8 = (s: string) => ENCODER.encode(s);
