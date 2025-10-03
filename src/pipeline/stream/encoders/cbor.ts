import { createChunkStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";
import { cborEncode } from "../../../deps.ts";

/**
 * CBOR streaming with record batching.
 *
 * Wire notes:
 * - Rows are batched into a single CBOR document shaped as:
 *     { rows: [ { statement: number, row: <object> }, ... ] }
 * - Non-row events are emitted as their own standalone CBOR documents.
 *
 * Batching policy:
 * - batchSize = flushEvery (if > 0) else 1024 (default)
 * - Flush pending rows BEFORE emitting any non-row event to preserve ordering.
 * - Always flush remaining rows on close().
 */
export function createCborEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, pushBytes, close } = createChunkStream(
    flushEvery,
    flushIntervalMs,
    onCancel,
    perf,
  );

  // Use flushEvery as the logical record batch size; fall back to 1024
  const batchSize = flushEvery > 0 ? flushEvery : 1024;

  type RowItem = { statement: number; row: unknown };
  let rowsBatch: RowItem[] = [];

  function hasBase64Sentinel(s: string) {
    return s.indexOf("base64:") !== -1;
  }

  function flushRowsBatch() {
    if (rowsBatch.length === 0) return;
    try {
      // Single CBOR document for the entire batch
      pushBytes(cborEncode({ rows: rowsBatch }));
    } finally {
      rowsBatch.length = 0;
    }
  }

  return {
    stream,
    emit: (_type, payload) => {
      // Preserve ordering: flush row batch before other events
      flushRowsBatch();
      pushBytes(cborEncode(payload));
    },
    emitRow: (i, rowJson) => {
      // Only revive binary if we actually see the "base64:" sentinel.
      let row: unknown;
      try {
        row = hasBase64Sentinel(rowJson)
          ? JSON.parse(rowJson, jsonReviverFast)
          : JSON.parse(rowJson);
      } catch {
        row = rowJson; // keep the stream valid in pathological cases
      }

      rowsBatch.push({ statement: i, row });
      if (rowsBatch.length >= batchSize) {
        flushRowsBatch();
      }
    },
    close: () => {
      try {
        flushRowsBatch();
      } finally {
        close();
      }
    },
  };
}

/** Inverse of jsonReplacer for base64:… only (typed binary revival). */
function jsonReviverFast(_k: string, v: unknown) {
  if (typeof v === "string" && v.startsWith("base64:")) {
    const b64 = v.slice(7);
    try {
      const bin = atob(b64);
      const u8 = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
      return u8;
    } catch {
      return v;
    }
  }
  return v as unknown;
}
