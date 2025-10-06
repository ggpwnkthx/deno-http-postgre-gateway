import type { ResponseFormat } from "./constants.ts";
import type { PerfCtx } from "./perf.ts";

export interface StatementSpec {
  sql: string;
  params?: unknown[];
}
export interface BatchBody {
  statements: StatementSpec[];
}

export interface PerIndexNumbers {
  [idx: number]: number;
}
export interface Timeouts {
  statementMs?: number;
  lockMs?: number;
  idleInTxnMs?: number;
  perIndexStatement?: PerIndexNumbers;
  perIndexLock?: PerIndexNumbers;
}

export interface Plan {
  id: string;
  appName?: string;
  dbUser: string;
  dbHost: string;
  dbName: string;
  fetchSize: number;
  flushEvery: number;
  flushIntervalMs: number;
  responseFormat: ResponseFormat;
  timeouts: Timeouts;
  statements: StatementSpec[];
  getClient: () => Promise<DbClient>;

  // Added for feature support:
  allowMutations: boolean; // whether non-select statements are permitted
  readOnlyBatch: boolean; // true if all statements are select-like
  autocommitReadOnly: boolean; // run read-only batches outside a transaction

  // Perf instrumentation (optional, no-op when disabled)
  perf?: PerfCtx;
}

export interface QuerySummary {
  command: string;
  rowCount: number | null;
}

/**
 * Stream rows from a server-side cursor.
 *
 * Modes:
 * - opts.as === "json": onRow receives a pre-serialized JSON string (zero parse).
 * - opts.as === "object": onRow receives a plain object with native values.
 * - opts.as === "array": onRow receives an array of column values (no per-row POJO).
 *
 * When available, the implementation SHOULD call onSchema(columns) once per statement
 * before streaming rows (for "array" or "object" modes that can leverage columns).
 */
export interface DbClient {
  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  execute(sql: string, params?: unknown[]): Promise<QuerySummary>;

  streamRows(
    sql: string,
    params: unknown[] | undefined,
    fetchSize: number,
    onRow: (row: Record<string, unknown> | string | unknown[]) => void,
    opts?: { as?: "json" | "object" | "array"; onSchema?: (columns: string[]) => void },
  ): Promise<QuerySummary>;

  end(): Promise<void>;
}
