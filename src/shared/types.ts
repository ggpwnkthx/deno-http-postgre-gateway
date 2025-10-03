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

export interface DbClient {
  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  execute(sql: string, params?: unknown[]): Promise<QuerySummary>;
  streamRows(
    sql: string,
    params: unknown[] | undefined,
    fetchSize: number,
    /**
     * If the encoder supports a fast path, it may pass a function that
     * accepts a **pre-serialized** JSON object string (no trailing newline).
     * Otherwise, a plain object is acceptable.
     */
    onRow: (row: Record<string, unknown> | string) => void,
  ): Promise<QuerySummary>;
  end(): Promise<void>;
}
