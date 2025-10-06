import { createChunkStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";
import {
  msgpackEncode,
  type msgpackValueMap as ValueMap,
  type msgpackValueType as ValueType,
} from "../../../deps.ts";

/**
 * MessagePack streaming with record batching (@std/msgpack).
 *
 * Modes:
 * - Receives native arrays/objects and directly encodes them (no JSON.parse).
 * - Binary columns (Uint8Array) stay binary.
 *
 * Batching:
 * - batchSize = flushEvery (if > 0) else 1024
 * - Remaining rows are flushed on close().
 */
export function createMsgpackEncoder(
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

  type RowItem = { statement: number; row: ValueType };
  const rowsBatch: RowItem[] = [];

  const flushRowsBatch = () => {
    if (!rowsBatch.length) return;
    try {
      const doc: ValueMap = { rows: rowsBatch as unknown as ReadonlyArray<ValueType> };
      pushBytes(msgpackEncode(doc));
    } finally {
      rowsBatch.length = 0;
    }
  };

  return {
    stream,

    // Array fast path
    emitArrayRow: (i, row) => {
      rowsBatch.push({ statement: i, row: row as ValueType });
      if (rowsBatch.length >= batchSize) flushRowsBatch();
    },

    // Compact stream shape: only "row" events are encoded.
    emit: (type, payload) => {
      if (type !== "row" || !payload || typeof payload !== "object") return;
      const { statement, row } = payload as { statement: number; row: unknown };
      rowsBatch.push({ statement, row: row as ValueType });
      if (rowsBatch.length >= batchSize) flushRowsBatch();
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
