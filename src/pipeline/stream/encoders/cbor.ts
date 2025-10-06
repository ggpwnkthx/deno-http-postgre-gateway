import { createChunkStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";
import { cborEncode } from "../../../deps.ts";

/**
 * CBOR streaming with record batching.
 *
 * Modes:
 * - Receives native arrays/objects; no JSON.parse. Binary stays Uint8Array.
 *
 * Batching:
 * - batchSize = flushEvery (if > 0) else 1024.
 * - Flush pending rows BEFORE emitting any non-row event to preserve ordering.
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
    undefined,
    onCancel,
    perf,
  );

  const batchSize = flushEvery > 0 ? flushEvery : 1024;

  type RowItem = { statement: number; row: unknown };
  const rowsBatch: RowItem[] = [];

  const flushRowsBatch = () => {
    if (!rowsBatch.length) return;
    try {
      pushBytes(cborEncode({ rows: rowsBatch }));
    } finally {
      rowsBatch.length = 0;
    }
  };

  return {
    stream,

    emitArrayRow: (i, row) => {
      rowsBatch.push({ statement: i, row });
      if (rowsBatch.length >= batchSize) flushRowsBatch();
    },

    emit: (type, payload) => {
      if (type === "row") {
        const p = payload as { statement: number; row: unknown };
        rowsBatch.push({ statement: p.statement, row: p.row });
        if (rowsBatch.length >= batchSize) flushRowsBatch();
        return;
      }
      // Preserve ordering for non-row events
      flushRowsBatch();
      pushBytes(cborEncode(payload));
    },

    preferArrays: true,
    preferObjects: true,

    close: () => {
      try {
        flushRowsBatch();
      } finally {
        close();
      }
    },
  };
}
