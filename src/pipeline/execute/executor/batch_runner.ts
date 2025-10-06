import type { Plan } from "../../../shared/types.ts";
import type { StreamEncoder } from "../../stream/encoders/types.ts";
import { runInStatementTxn, setLocalReadOnly, setLocalTimeouts } from "./tx.ts";
import { executeStatement } from "./statement.ts";

/**
 * Drives the autocommit vs single-transaction strategy and per-statement execution.
 * Returns the DB duration in milliseconds to report in the final batch event.
 */
export async function runBatch(args: {
  plan: Plan;
  client: Awaited<ReturnType<Plan["getClient"]>>;
  writer: StreamEncoder;
  state: { inTx: boolean; cancelled: boolean };
}): Promise<number> {
  const { plan, client, writer, state } = args;

  const start = performance.now();
  const readOnlyBatch = plan.readOnlyBatch;

  // "autocommit" == run each statement in its own small txn (cursor-legal, pooling-safe)
  const autocommit = !!plan.autocommitReadOnly && readOnlyBatch;

  // Enforce DB-level read-only if the batch is read-only and mutations are not allowed.
  const enforceReadOnly = readOnlyBatch && !plan.allowMutations;

  const spanBatch = plan.perf?.span("batch", {
    autocommit,
    readOnlyBatch,
    statements: plan.statements.length,
  });

  if (!autocommit) {
    const spanBegin = plan.perf?.span("batch.begin_tx");
    await client.begin();
    state.inTx = true;

    await setLocalReadOnly(client, enforceReadOnly);
    await setLocalTimeouts(client, {
      statementMs: plan.timeouts.statementMs,
      lockMs: plan.timeouts.lockMs,
      idleInTxnMs: plan.timeouts.idleInTxnMs,
    });
    spanBegin?.end();
  }

  for (let i = 0; i < plan.statements.length; i++) {
    // Do **not** break on state.cancelled: complete DB work deterministically.
    // The stream may already be closed by the client, but DB-side work should finish.

    // Effective per-statement timeouts (per-index overrides → global defaults)
    const perStmt = plan.timeouts.perIndexStatement?.[i] ?? plan.timeouts.statementMs;
    const perLock = plan.timeouts.perIndexLock?.[i] ?? plan.timeouts.lockMs;
    const perTimeouts = {
      statementMs: perStmt,
      lockMs: perLock,
      idleInTxnMs: plan.timeouts.idleInTxnMs,
    };

    const spec = plan.statements[i];
    const spanStmt = plan.perf?.span("batch.statement", {
      index: i,
      sqlLength: spec.sql.length,
    });

    const doWork = async () => {
      // Even if cancelled, execute to keep transactional semantics/logging intact.
      await executeStatement({
        client,
        plan,
        writer,
        index: i,
      });
    };

    if (autocommit) {
      const spanTxn = plan.perf?.span("batch.statement.autocommit", { index: i });
      await runInStatementTxn(client, perTimeouts, doWork, enforceReadOnly);
      spanTxn?.end();
    } else {
      await setLocalTimeouts(client, perTimeouts);
      await doWork();
    }
    spanStmt?.end();
  }

  if (!autocommit) {
    const spanCommit = plan.perf?.span("batch.commit_tx");
    await client.commit();
    state.inTx = false;
    spanCommit?.end();
  }

  const elapsed = Math.round(performance.now() - start);
  spanBatch?.mark("elapsedMs", elapsed);
  spanBatch?.end();
  return elapsed;
}
