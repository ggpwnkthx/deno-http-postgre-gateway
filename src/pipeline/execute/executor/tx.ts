import type { Plan } from "../../../shared/types.ts";

/**
 * Apply timeouts using SET LOCAL (transaction-scoped only).
 * Sends a single simple-protocol query when >1 setting is present to reduce rtts.
 */
export async function setLocalTimeouts(
  client: Awaited<ReturnType<Plan["getClient"]>>,
  t: { statementMs?: number; lockMs?: number; idleInTxnMs?: number },
) {
  const stmts: string[] = [];
  if (typeof t.statementMs === "number") {
    stmts.push(`SET LOCAL statement_timeout = ${Math.max(0, t.statementMs)};`);
  }
  if (typeof t.lockMs === "number") {
    stmts.push(`SET LOCAL lock_timeout = ${Math.max(0, t.lockMs)};`);
  }
  if (typeof t.idleInTxnMs === "number") {
    stmts.push(`SET LOCAL idle_in_transaction_session_timeout = ${Math.max(0, t.idleInTxnMs)};`);
  }
  if (stmts.length > 0) {
    await client.execute(stmts.join(" "));
  }
}

/** Enforce transaction-scoped read-only when desired (pooling-safe). */
export async function setLocalReadOnly(
  client: Awaited<ReturnType<Plan["getClient"]>>,
  enforce: boolean,
) {
  if (enforce) {
    await client.execute("SET LOCAL transaction_read_only = on;");
  }
}

/** Run a single statement (or cursor stream) inside its own tiny transaction. */
export async function runInStatementTxn(
  client: Awaited<ReturnType<Plan["getClient"]>>,
  timeouts: { statementMs?: number; lockMs?: number; idleInTxnMs?: number },
  fn: () => Promise<void>,
  readOnlyGuard: boolean,
) {
  await client.begin();
  try {
    await setLocalReadOnly(client, readOnlyGuard);
    await setLocalTimeouts(client, timeouts);
    await fn();
    await client.commit();
  } catch (e) {
    try {
      await client.rollback();
    } catch { /* ignore */ }
    throw e;
  }
}
