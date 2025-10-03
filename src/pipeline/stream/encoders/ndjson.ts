import { createLineStream, jsonReplacer } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

export function createNdjsonEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, push, close } = createLineStream(
    flushEvery,
    flushIntervalMs,
    undefined,
    onCancel,
    perf,
  );

  // Cache the small per-statement wrapper prefix
  const rowPrefixCache = new Map<number, string>();
  const getRowPrefix = (i: number) => {
    let p = rowPrefixCache.get(i);
    if (!p) {
      p = `{"statement":${i},"row":`;
      rowPrefixCache.set(i, p);
    }
    return p;
  };

  return {
    stream,
    emit: (_t, payload) => {
      // Keep original shape; just a single push
      push(JSON.stringify(payload, jsonReplacer) + "\n");
    },
    emitRow: (i, rowJson) => {
      // Avoid object alloc and extra stringify per row
      push(getRowPrefix(i) + rowJson + "}\n");
    },
    close,
  };
}
