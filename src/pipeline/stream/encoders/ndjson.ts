import { createChunkStream, encodeUtf8, jsonReplacer } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

/**
 * NDJSON encoder with near zero-copy framing:
 * - Emits three byte chunks per row: prefix(bytes), rowJson(bytes), suffix(bytes).
 * - Metadata/events are encoded once per emit without string concatenation.
 */
export function createNdjsonEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, pushBytes, close } = createChunkStream(
    flushEvery,
    flushIntervalMs,
    undefined,
    onCancel,
    perf,
  );

  const SUFFIX = encodeUtf8("}\n");
  const NL = encodeUtf8("\n");

  const rowPrefixCache = new Map<number, Uint8Array>();
  const rowPrefix = (i: number) => {
    let u8 = rowPrefixCache.get(i);
    if (!u8) {
      u8 = encodeUtf8(`{"statement":${i},"row":`);
      rowPrefixCache.set(i, u8);
    }
    return u8;
  };

  return {
    stream,

    // General events (summary, begin, etc.)
    emit: (_t, payload) => {
      try {
        pushBytes(encodeUtf8(JSON.stringify(payload, jsonReplacer)));
        pushBytes(NL);
      } catch {
        // best effort
      }
    },

    // Fast path for pre-serialized JSON row
    emitRow: (i, rowJson) => {
      try {
        pushBytes(rowPrefix(i));
        pushBytes(encodeUtf8(rowJson));
        pushBytes(SUFFIX);
      } catch {
        // best effort
      }
    },

    close,
  };
}
