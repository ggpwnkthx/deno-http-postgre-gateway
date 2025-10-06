// Minimal, deterministic mock implementing DbClient.
// Lets you feed SELECT rows (as JSON strings) and control execute() summaries.
import type { DbClient, QuerySummary } from "../../src/shared/types.ts";

export type MockPlan = {
  // For streamRows: sql -> rows (each row is already JSON string)
  rowSets?: Record<string, string[]>;
  // For execute: sql -> summary (falls back to SELECT/QUERY heuristic)
  summaries?: Record<string, QuerySummary>;
  // Throw on execute/stream when sql matches this regex (optional)
  failWhenSqlMatches?: RegExp;
  // Optional delay per FETCH (ms)
  fetchDelayMs?: number;
};

export class MockDbClient implements DbClient {
  public begun = 0;
  public committed = 0;
  public rolledBack = 0;
  public ended = 0;
  public calls: Array<{ op: string; sql?: string }> = [];

  constructor(private plan: MockPlan = {}) {}

  begin(): Promise<void> {
    this.calls.push({ op: "BEGIN" });
    this.begun++;
    return Promise.resolve();
  }

  commit(): Promise<void> {
    this.calls.push({ op: "COMMIT" });
    this.committed++;
    return Promise.resolve();
  }

  rollback(): Promise<void> {
    this.calls.push({ op: "ROLLBACK" });
    this.rolledBack++;
    return Promise.resolve();
  }

  execute(sql: string, _params?: unknown[]): Promise<QuerySummary> {
    this.calls.push({ op: "EXEC", sql });
    if (this.plan.failWhenSqlMatches?.test(sql)) {
      return Promise.reject(new Error("forced_execute_failure"));
    }
    const fromMap = this.plan.summaries?.[sql];
    if (fromMap) return Promise.resolve(fromMap);
    const cmd = /^\s*select|values|table|show|explain/i.test(sql) ? "SELECT" : "QUERY";
    return Promise.resolve({ command: cmd, rowCount: null });
  }

  async streamRows(
    sql: string,
    _params: unknown[] | undefined,
    fetchSize: number,
    onRow: (row: Record<string, unknown> | string) => void,
  ): Promise<QuerySummary> {
    this.calls.push({ op: "DECLARE/FETCH", sql });
    if (this.plan.failWhenSqlMatches?.test(sql)) {
      throw new Error("forced_stream_failure");
    }

    const rows = this.plan.rowSets?.[sql] ?? [];
    let i = 0;
    while (i < rows.length) {
      const end = Math.min(i + fetchSize, rows.length);
      for (let j = i; j < end; j++) {
        onRow(rows[j]); // fast path: pre-serialized JSON string
      }
      i = end;
      if (this.plan.fetchDelayMs) {
        await new Promise((r) => setTimeout(r, this.plan.fetchDelayMs));
      }
    }
    return { command: "SELECT", rowCount: rows.length };
  }

  end(): Promise<void> {
    this.calls.push({ op: "END" });
    this.ended++;
    return Promise.resolve();
  }
}
