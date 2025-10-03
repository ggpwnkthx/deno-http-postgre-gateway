import type { PerfCtx } from "../../../shared/perf.ts";

// Single shared encoder avoids per-stream allocations.
const ENCODER = new TextEncoder();

type Mode = "line" | "chunk";

class Buffered {
  private readonly mode: Mode;
  private strMode: boolean;
  private closed = false;
  private timer: number | null = null;

  // byte-buffering mode (chunk)
  private chunksU8: Uint8Array[] = [];
  private totalBytes = 0;

  // line-buffering mode (strings → encode-on-flush)
  private chunksStr: string[] = [];

  // perf
  private bytesOut = 0;
  private linesOut = 0;
  private flushes = 0;

  constructor(
    private controller: ReadableStreamController<Uint8Array>,
    private flushEvery: number,
    private intervalMs: number,
    private onStart: ((push: (s: string) => void) => void) | undefined,
    private onCancel: (() => void) | undefined,
    private perf: PerfCtx | undefined,
    mode: Mode,
  ) {
    this.mode = mode;

    // strMode = flushEvery === 0 → push immediately (lowest latency)
    this.strMode = this.flushEvery === 0;

    if (this.intervalMs > 0) {
      this.timer = setInterval(() => this.flush(), this.intervalMs) as unknown as number;
    }
    if (this.onStart) this.onStart((s) => this.pushLine(s));
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

    if (this.mode === "line") {
      // Defensive: some encoders may call pushBytes in line mode (rare)
      if (this.strMode) {
        this.enqueueBytes(u8);
        this.bytesOut += u8.byteLength;
        return;
      }
      this.chunksU8.push(u8);
      this.totalBytes += u8.byteLength;
      if (this.flushEvery > 0 && this.chunksU8.length >= this.flushEvery) {
        this.flush();
      }
      return;
    }

    // chunk mode
    if (this.strMode) {
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

  pushLine(line: string) {
    if (this.closed) return;

    if (this.strMode) {
      const u8 = ENCODER.encode(line);
      this.enqueueBytes(u8);
      this.bytesOut += u8.byteLength;
      this.linesOut++;
      return;
    }

    if (this.mode === "line") {
      // Buffer strings and encode once per flush
      this.chunksStr.push(line);
      this.linesOut++;
      if (this.flushEvery > 0 && this.chunksStr.length >= this.flushEvery) {
        this.flush();
      }
      return;
    }

    // Fallback: chunk mode but given a line — encode now but batch later
    const u8 = ENCODER.encode(line);
    this.chunksU8.push(u8);
    this.totalBytes += u8.byteLength;
    this.linesOut++;
    if (this.flushEvery > 0 && this.chunksU8.length >= this.flushEvery) {
      this.flush();
    }
  }

  flush() {
    if (this.closed) return;
    if (this.strMode) return; // no buffering in strMode

    if (this.mode === "line") {
      if (this.chunksStr.length === 0) return;
      const span = this.perf?.span("stream.flush", {
        chunks: this.chunksStr.length,
      });
      // Join once → encode once
      const joined = this.chunksStr.join("");
      this.chunksStr.length = 0;
      const u8 = ENCODER.encode(joined);
      this.enqueueBytes(u8);
      this.bytesOut += u8.byteLength;
      this.flushes++;
      span?.mark("bytes", u8.byteLength);
      span?.end();
      return;
    }

    // chunk mode (bytes)
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

export function createLineStream(
  flushEvery: number,
  flushIntervalMs: number,
  onStart?: (push: (s: string) => void) => void,
  onCancel?: () => void,
  perf?: PerfCtx,
) {
  let impl: Buffered;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      impl = new Buffered(
        controller,
        flushEvery,
        flushIntervalMs,
        onStart,
        onCancel,
        perf,
        "line",
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

export function createChunkStream(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
) {
  let impl: Buffered;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      impl = new Buffered(
        controller,
        flushEvery,
        flushIntervalMs,
        undefined,
        onCancel,
        perf,
        "chunk",
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

export function jsonReplacer(_key: string, value: unknown) {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) {
    let bin = "";
    for (let i = 0; i < value.length; i++) bin += String.fromCharCode(value[i]);
    return `base64:${btoa(bin)}`;
  }
  if (value instanceof ArrayBuffer) {
    const view = new Uint8Array(value);
    let bin = "";
    for (let i = 0; i < view.length; i++) bin += String.fromCharCode(view[i]);
    return `base64:${btoa(bin)}`;
  }
  return value as unknown;
}
