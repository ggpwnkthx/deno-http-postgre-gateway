import { createLineStream, jsonReplacer } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

export function createSseEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, push, close } = createLineStream(
    flushEvery,
    flushIntervalMs,
    (emit) => emit(":ok\n\n"),
    onCancel,
    perf,
  );

  // Cache tiny immutable prefixes per-statement to avoid rebuilding strings
  const rowPrefixCache = new Map<number, string>();
  const getRowPrefix = (i: number) => {
    let p = rowPrefixCache.get(i);
    if (!p) {
      p = `event: row\ndata: {"statement":${i},"row":`;
      rowPrefixCache.set(i, p);
    }
    return p;
  };

  return {
    stream,
    emit: (type, payload) => {
      // Coalesce into a single push to reduce encode/enqueue overhead
      push(`event: ${type}\ndata: ${JSON.stringify(payload, jsonReplacer)}\n\n`);
    },
    emitRow: (i, rowJson) => {
      // Single push (was two) + cached prefix
      push(getRowPrefix(i) + rowJson + "}\n\n");
    },
    close,
  };
}
