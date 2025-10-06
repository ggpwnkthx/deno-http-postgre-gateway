// src/pipeline/stream/encoders/arrow.ts
import { createChunkStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";
import { RecordBatchWriter, tableFromJSON } from "../../../deps.ts";

/**
 * Apache Arrow IPC streaming encoder with record batching.
 *
 * Perf characteristics:
 * - Batches arrays/objects/JSON-strings; single parse per JSON batch.
 * - Byte-aware early flush for JSON accumulation to cap peak memory.
 * - Larger default batch size when flushEvery=0 (protected by byte ceiling).
 *
 * Inputs:
 *  - onSchema(i, cols)           -> names for array rows
 *  - emitArrayRow(i, any[])      -> array rows; mapped to objects per batch
 *  - emit("row", {row: object})  -> object rows
 *  - emitRow(i, jsonString)      -> JSON-string rows; single-parse per batch
 */
export function createArrowIpcEncoder(
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

  // Batch sizing:
  const BATCH_ROWS = flushEvery > 0 ? flushEvery : 4096;

  // Cap JSON accumulation to avoid building huge intermediate strings.
  const JSON_MAX_BYTES = 8 * 1024 * 1024; // 8 MiB

  type Buckets = {
    cols?: string[];
    arrayRows: unknown[][];
    objectRows?: Record<string, unknown>[];
    jsonRows?: string[];
    jsonBytes: number;
  };

  const byStmt = new Map<number, Buckets>();

  const ensureBucket = (i: number): Buckets => {
    let b = byStmt.get(i);
    if (!b) {
      b = { arrayRows: [], jsonBytes: 0 };
      byStmt.set(i, b);
    }
    return b;
  };

  // --- Conversions -----------------------------------------------------------

  function toObjectsFromArrays(cols: string[] | undefined, rows: unknown[][]) {
    if (rows.length === 0) return [] as Record<string, unknown>[];
    const names = cols ?? rows[0].map((_v, idx) => `col${idx + 1}`);
    const out = new Array<Record<string, unknown>>(rows.length);
    for (let r = 0; r < rows.length; r++) {
      const row = rows[r];
      const obj: Record<string, unknown> = {};
      for (let c = 0; c < names.length; c++) obj[names[c]] = row[c];
      out[r] = obj;
    }
    return out;
  }

  /** Single-parse JSON rows per batch via `JSON.parse("[" + rows.join(",") + "]")`. */
  function toObjectsFromJsonBatch(jsonRows: string[]) {
    if (jsonRows.length === 0) return [] as Record<string, unknown>[];
    const asArray = `[${jsonRows.join(",")}]`;
    try {
      const parsed = JSON.parse(asArray) as Array<unknown>;
      for (let i = 0; i < parsed.length; i++) {
        const v = parsed[i];
        if (!v || typeof v !== "object" || Array.isArray(v)) {
          parsed[i] = { value: v as unknown };
        }
      }
      return parsed as Record<string, unknown>[];
    } catch {
      // Fallback per-row parse for robustness.
      const out = new Array<Record<string, unknown>>(jsonRows.length);
      for (let i = 0; i < jsonRows.length; i++) {
        try {
          const v = JSON.parse(jsonRows[i]);
          out[i] = v && typeof v === "object" && !Array.isArray(v)
            ? (v as Record<string, unknown>)
            : { value: v as unknown };
        } catch {
          out[i] = { value: jsonRows[i] as unknown };
        }
      }
      return out;
    }
  }

  // --- Serialization ---------------------------------------------------------

  function emitTableIpc(rows: Record<string, unknown>[]) {
    if (!rows.length) return;

    let u8: Uint8Array | null = null;

    try {
      const table = tableFromJSON(rows);

      // Strategy 1: Prefer Table.serialize({ format: "stream" }) if available.
      const anyTable = table as unknown as { serialize?: (opts?: unknown) => Uint8Array };
      if (typeof anyTable?.serialize === "function") {
        try {
          u8 = anyTable.serialize({ format: "stream" });
        } catch {
          try {
            u8 = anyTable.serialize();
          } catch {
            // fall through to writer fallback
          }
        }
      }

      // Strategy 2: RecordBatchWriter fallback.
      if (!u8) {
        try {
          const writer = new RecordBatchWriter();
          writer.writeAll(table);
          if (typeof writer?.toUint8Array === "function") {
            u8 = writer.toUint8Array(true); // finalize + contiguous buffer
          }
        } catch {
          // give up gracefully
        }
      }
    } catch {
      // tableFromJSON failed; keep u8 null
    }

    if (u8 && u8.byteLength) pushBytes(u8);
  }

  // --- Flushing --------------------------------------------------------------

  function flushStatementBatch(i: number) {
    const b = byStmt.get(i);
    if (!b) return;

    // Priority: arrays -> objects -> json
    if (b.arrayRows.length) {
      const objs = toObjectsFromArrays(b.cols, b.arrayRows);
      b.arrayRows.length = 0;
      emitTableIpc(objs);
      return;
    }
    if (b.objectRows?.length) {
      const objs = b.objectRows;
      b.objectRows = [];
      emitTableIpc(objs);
      return;
    }
    if (b.jsonRows?.length) {
      const rows = b.jsonRows;
      b.jsonRows = [];
      b.jsonBytes = 0;
      emitTableIpc(toObjectsFromJsonBatch(rows));
    }
  }

  function flushStatementAll(i: number) {
    flushStatementBatch(i);
    byStmt.delete(i);
  }

  // --- Encoder ---------------------------------------------------------------

  return {
    stream,

    onSchema: (i, columns) => {
      ensureBucket(i).cols = columns.slice();
    },

    emitArrayRow: (i, row) => {
      const b = ensureBucket(i);
      b.arrayRows.push(row);
      if (b.arrayRows.length >= BATCH_ROWS) flushStatementBatch(i);
    },

    emitRow: (i, rowJson) => {
      const b = ensureBucket(i);
      if (!b.jsonRows) {
        b.jsonRows = [];
        b.jsonBytes = 0;
      }
      b.jsonRows.push(rowJson);
      b.jsonBytes += rowJson.length + 1; // include comma
      if (b.jsonRows.length >= BATCH_ROWS || b.jsonBytes >= JSON_MAX_BYTES) {
        flushStatementBatch(i);
      }
    },

    emit: (type, payload) => {
      if (type === "row" && payload && typeof payload === "object") {
        const { statement, row } = payload as { statement: number; row: unknown };
        const b = ensureBucket(statement);
        const r = row && typeof row === "object" && !Array.isArray(row)
          ? (row as Record<string, unknown>)
          : ({ value: row as unknown } as Record<string, unknown>);
        (b.objectRows ??= []).push(r);
        if (b.objectRows.length >= BATCH_ROWS) flushStatementBatch(statement);
        return;
      }

      // Per-statement summary triggers full flush
      if (type === "summary" && payload && typeof payload === "object") {
        const idx = (payload as { statement?: unknown }).statement;
        if (typeof idx === "number") flushStatementAll(idx);
      }
    },

    // Prefer the most efficient upstream delivery when available.
    preferArrays: true,
    preferObjects: true,

    close: () => {
      try {
        for (const i of byStmt.keys()) flushStatementAll(i);
      } finally {
        close();
      }
    },
  };
}
