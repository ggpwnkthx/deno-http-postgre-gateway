import type { Plan, QuerySummary } from "../../../shared/types.ts";

export const evt = {
  begin: (p: Plan) => ({
    batch: { begin: true, statements: p.statements.length, requestId: p.id },
  }),
  committed: (ms: number) => ({ batch: { committed: true, dbDurationMs: ms } }),
  aborted: () => ({ batch: { aborted: true } }),
  row: (i: number, row: Record<string, unknown>) => ({ statement: i, row }),
  summary: (i: number, s: QuerySummary) => ({ statement: i, summary: s }),
  error: (i: number | null, e: unknown) => ({
    error: String(e instanceof Error ? e.message : e),
    statement: i,
  }),
};
