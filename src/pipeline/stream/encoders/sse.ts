import { createChunkStream, encodeUtf8, jsonReplacer } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

/**
 * SSE encoder with near zero-copy framing:
 * - Uses onStart hook to emit ":ok\n\n" without extra allocations.
 * - For events: emits "event: " + type + "\n" + "data: " + json + "\n\n"
 *   as separate byte chunks (no string concatenation).
 * - Row fast-path: uses pre-encoded per-statement prefix to avoid rebuilds.
 */
export function createSseEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const COLON_OK = encodeUtf8(":ok\n\n");
  const EVENT_PFX = encodeUtf8("event: ");
  const NL = encodeUtf8("\n");
  const DATA_PFX = encodeUtf8("data: ");
  const DBL_NL = encodeUtf8("\n\n");
  const ROW_SUFFIX = encodeUtf8("}\n\n");

  const rowPrefixCache = new Map<number, Uint8Array>();
  const rowPrefix = (i: number) => {
    let u8 = rowPrefixCache.get(i);
    if (!u8) {
      u8 = encodeUtf8(`event: row\ndata: {"statement":${i},"row":`);
      rowPrefixCache.set(i, u8);
    }
    return u8;
  };

  const { stream, pushBytes, close } = createChunkStream(
    flushEvery,
    flushIntervalMs,
    // onStart: prelude keep-alive/comment
    (push) => push(COLON_OK),
    onCancel,
    perf,
  );

  return {
    stream,

    emit: (type, payload) => {
      try {
        pushBytes(EVENT_PFX);
        pushBytes(encodeUtf8(type));
        pushBytes(NL);

        pushBytes(DATA_PFX);
        pushBytes(encodeUtf8(JSON.stringify(payload, jsonReplacer)));
        pushBytes(DBL_NL);
      } catch {
        // best effort
      }
    },

    emitRow: (i, rowJson) => {
      try {
        pushBytes(rowPrefix(i));
        pushBytes(encodeUtf8(rowJson));
        pushBytes(ROW_SUFFIX);
      } catch {
        // best effort
      }
    },

    close,
  };
}
