/**
 * Minimal perf instrumentation with near-zero cost when disabled.
 * - Enable globally via PERF_DEBUG=1 (or true/yes/on) or per-request via header "x-perf-debug".
 * - Optional sampling via PERF_SAMPLE_RATE (0..1, default 1 when enabled).
 * - Logs structured JSON lines (level=debug, event=perf) on span end.
 * - Includes requestId when provided and best-effort memory deltas.
 *
 * New:
 * - Optional `logger` to control emission (defaults to structured console.log).
 * - Env `PERF_SILENT=1` to suppress all perf logs without code changes.
 */

export type PerfNumber = number | bigint;

export interface PerfSpan {
  /** span name, e.g. "batch", "statement", "stream" */
  name: string;
  /** mark an attribute (counter or field) */
  mark(key: string, value: unknown): void;
  /** increment numeric attribute (created if missing) */
  incr(key: string, delta?: PerfNumber): void;
  /** finish the span and emit a log if it clears threshold */
  end(extra?: Record<string, unknown>): void;
  /** child span (inherits ctx, requestId, threshold) */
  child(name: string, data?: Record<string, unknown>): PerfSpan;
}

export interface PerfCtx {
  enabled: boolean;
  requestId?: string;
  minDurationMs: number;
  /** start a new span */
  span(name: string, data?: Record<string, unknown>): PerfSpan;
}

/** Parse boolean-like strings. */
function parseBool(v: string | undefined | null, def = false): boolean {
  const s = (v ?? "").trim().toLowerCase();
  if (!s) return def;
  return ["1", "true", "yes", "y", "on"].includes(s);
}

function now() {
  return performance.now();
}

function tryGetHeapUsed(): number | null {
  try {
    // Deno 2.x: Deno.memoryUsage?.() is not guaranteed; guard by try/catch.
    const anyD = Deno as unknown as { memoryUsage?: () => { heapUsed?: number } };
    const mu = anyD.memoryUsage?.();
    return typeof mu?.heapUsed === "number" ? mu.heapUsed : null;
  } catch {
    return null;
  }
}

function rnd() {
  return Math.random();
}

function shouldSample(sampleRate: number) {
  if (sampleRate >= 1) return true;
  if (sampleRate <= 0) return false;
  return rnd() < sampleRate;
}

export interface PerfCreateOptions {
  enabled?: boolean;
  requestId?: string;
  minDurationMs?: number;
  sampleRate?: number;
  /**
   * If provided, used to emit the final perf record.
   * Pass a no-op (e.g. () => {}) to silence logs.
   * If omitted, defaults to structured console.log(JSON.stringify(record)).
   */
  logger?: (record: Record<string, unknown>) => void;
}

export function createPerfCtx(opts?: PerfCreateOptions): PerfCtx {
  const envEnabled = parseBool(Deno.env.get("PERF_DEBUG"));
  const enabled = !!(opts?.enabled ?? envEnabled);

  // Default logger → keep existing behavior: JSON line to console.log
  const envSilent = parseBool(Deno.env.get("PERF_SILENT"));
  const defaultLogger = (rec: Record<string, unknown>) => {
    try {
      console.log(JSON.stringify(rec));
    } catch {
      // best-effort
    }
  };
  const logger = opts?.logger ?? (envSilent ? undefined : defaultLogger);

  if (!enabled) {
    // No-op context to eliminate branching elsewhere.
    const noopSpan: PerfSpan = {
      name: "noop",
      mark: () => {},
      incr: () => {},
      end: () => {},
      child: () => noopSpan,
    };
    return {
      enabled: false,
      minDurationMs: Infinity,
      requestId: opts?.requestId,
      span: () => noopSpan,
    };
  }

  const sampleRate = Number(Deno.env.get("PERF_SAMPLE_RATE") ?? (opts?.sampleRate ?? 1));
  if (!shouldSample(sampleRate)) {
    const noopSpan: PerfSpan = {
      name: "noop",
      mark: () => {},
      incr: () => {},
      end: () => {},
      child: () => noopSpan,
    };
    return {
      enabled: false,
      minDurationMs: Infinity,
      requestId: opts?.requestId,
      span: () => noopSpan,
    };
  }

  const minDurationMs = Math.max(
    0,
    Number(Deno.env.get("PERF_MIN_MS") ?? (opts?.minDurationMs ?? 0)),
  );

  function startSpan(
    name: string,
    data?: Record<string, unknown>,
    requestId?: string,
  ): PerfSpan {
    const t0 = now();
    const heap0 = tryGetHeapUsed();
    const marks: Record<string, unknown> = data ? { ...data } : {};

    function emit(durationMs: number, extra?: Record<string, unknown>) {
      if (durationMs < minDurationMs) return;
      if (!logger) return; // silenced (either env PERF_SILENT=1 or user-provided no-op)
      const heap1 = tryGetHeapUsed();
      const rec: Record<string, unknown> = {
        level: "debug",
        event: "perf",
        name,
        durationMs: Math.round(durationMs * 1000) / 1000,
        ...(requestId ? { requestId } : {}),
        ...(heap0 !== null && heap1 !== null ? { heapDelta: heap1 - heap0, heapUsed: heap1 } : {}),
        ...marks,
        ...(extra ?? {}),
      };
      try {
        logger(rec);
      } catch {
        // best-effort
      }
    }

    return {
      name,
      mark(k, v) {
        marks[k] = v;
      },
      incr(k, delta = 1) {
        const cur = marks[k];
        if (typeof cur === "number") {
          marks[k] = (cur as number) + Number(delta);
        } else if (typeof cur === "bigint") {
          marks[k] = (cur as bigint) + BigInt(delta as number | bigint);
        } else {
          marks[k] = Number(delta);
        }
      },
      end(extra) {
        emit(now() - t0, extra);
      },
      child(childName: string, childData?: Record<string, unknown>): PerfSpan {
        return startSpan(`${name}.${childName}`, childData, requestId);
      },
    };
  }

  return {
    enabled: true,
    minDurationMs,
    requestId: opts?.requestId,
    span(name, data) {
      return startSpan(name, data, opts?.requestId);
    },
  };
}
