import type { Plan } from "../../../shared/types.ts";

export function logBatchDone(plan: Plan, durationMs: number) {
  try {
    console.log(JSON.stringify({
      level: "info",
      event: "batch_done",
      requestId: plan.id,
      dbUser: plan.dbUser,
      dbHost: plan.dbHost,
      dbName: plan.dbName,
      appName: plan.appName ?? null,
      readOnlyBatch: plan.readOnlyBatch,
      autocommit: !!(plan.autocommitReadOnly && plan.readOnlyBatch),
      durationMs,
      statements: plan.statements.length,
    }));
  } catch {
    // swallow logging errors
  }
}

export function logBatchError(plan: Plan, e: unknown) {
  try {
    console.error(JSON.stringify({
      level: "error",
      event: "batch_error",
      requestId: plan.id,
      message: e instanceof Error ? e.message : String(e),
    }));
  } catch {
    // swallow logging errors
  }
}
