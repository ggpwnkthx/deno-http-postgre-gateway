import { createChunkStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";
import { RecordBatchWriter, tableFromJSON } from "../../../deps.ts";

/**
 * Apache Arrow IPC streaming encoder with record batching.
 *
 * Strategy:
 *  - Accumulate rows per statement (as plain JS objects).
 *  - When a per-statement buffer reaches BATCH_ROWS, convert that slice to a Table,
 *    write it through a RecordBatchWriter (IPC stream format), and enqueue produced chunks.
 *  - On the statement's 'summary' event, flush any remaining rows.
 *  - Non-row events are ignored (Arrow is row-data centric).
 *
 * Notes:
 *  - We intentionally create a new RecordBatchWriter per flushed batch. This yields
 *    multiple IPC streams back-to-back for a statement. Most consumers can read
 *    concatenated streams; if you need a single logical stream per statement,
 *    consider a future refactor that keeps one writer open per statement and
 *    buffers chunks until `.finish()`.
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
    onCancel,
    perf,
  );

  // Pick batch size: prefer flushEvery when provided, else a sensible default.
  const BATCH_ROWS = flushEvery > 0 ? flushEvery : 2048;

  // Accumulate rows by statement index
  const rowsByStmt = new Map<number, Array<Record<string, unknown>>>();

  async function flushStatementBatch(i: number) {
    const rows = rowsByStmt.get(i);
    if (!rows || rows.length === 0) return;

    // Convert just this batch of JSON rows to an Arrow Table
    const table = tableFromJSON(rows);

    // Emit a new IPC stream for this table
    const writer = new RecordBatchWriter();
    await writer.writeAll(table).finish();

    for await (const chunk of writer) {
      pushBytes(chunk);
    }

    // Reset buffer for this statement
    rows.length = 0;
  }

  async function flushStatementAll(i: number) {
    // Best-effort: flush any rows left for this statement
    await flushStatementBatch(i);
    rowsByStmt.delete(i);
  }

  return {
    stream,
    emit: (type, payload) => {
      // When summary for a statement arrives, emit its remaining Arrow batches
      if (type === "summary" && payload && typeof payload === "object") {
        const idx = (payload as { statement?: unknown }).statement;
        if (typeof idx === "number") {
          // Fire and forget within the same microtask queue.
          void flushStatementAll(idx);
        }
      }
      // Other events are ignored for Arrow.
    },
    emitRow: (i, rowJson) => {
      try {
        const obj = JSON.parse(rowJson) as Record<string, unknown>;
        let arr = rowsByStmt.get(i);
        if (!arr) {
          arr = [];
          rowsByStmt.set(i, arr);
        }
        arr.push(obj);

        // Batch flush once we reach BATCH_ROWS for this statement
        if (arr.length >= BATCH_ROWS) {
          // Flush current batch and keep buffering new rows afterwards
          // Note: we copy the array reference and replace with a fresh one
          const toFlush = arr.slice(0);
          rowsByStmt.set(i, []);
          (async () => {
            const table = tableFromJSON(toFlush);
            const writer = new RecordBatchWriter();
            await writer.writeAll(table).finish();
            for await (const chunk of writer) {
              pushBytes(chunk);
            }
          })();
        }
      } catch {
        // ignore malformed rows (shouldn't happen)
      }
    },
    close: () => {
      // Best-effort flush for any statements that didn't see a summary (errors/early close).
      const pending = Array.from(rowsByStmt.keys());
      (async () => {
        for (const i of pending) {
          await flushStatementAll(i);
        }
        close();
      })();
    },
  };
}
