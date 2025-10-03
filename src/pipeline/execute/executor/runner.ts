import type { Plan } from "../../../shared/types.ts";
import { createStreamEncoder, respondWithStream } from "../../stream/encoder.ts";
import { evt } from "./events.ts";
import { runBatch } from "./batch_runner.ts";
import { makeOnCancel } from "./cancel.ts";
import { logBatchDone, logBatchError } from "./logs.ts";

export function executePlan(plan: Plan): Response {
  // We'll acquire the client inside the async runner. `handleBatch` already
  // pre-connects and shapes connect errors before calling us.
  let client: Awaited<ReturnType<Plan["getClient"]>> | null = null;

  // Stream encoder with robust cancel handler (rollback/end + mark cancelled)
  const state = { inTx: false, cancelled: false };
  const writer = createStreamEncoder(
    plan.responseFormat,
    plan.flushEvery,
    plan.flushIntervalMs,
    makeOnCancel(() => client, state),
    plan.perf, // <-- pass perf into encoder for stream metrics
  );

  queueMicrotask(() => writer.emit("batch", evt.begin(plan)));

  (async () => {
    const spanExec = plan.perf?.span("execute") ?? null;
    try {
      // Acquire the preconnected client (or connect if a different caller provided a lazy factory).
      const spanClient = plan.perf?.span("execute.acquire_client");
      client = await plan.getClient();
      spanClient?.end();

      const dbDurationMs = await runBatch({
        plan,
        client,
        writer,
        state,
      });

      writer.emit("batch", evt.committed(dbDurationMs));
      logBatchDone(plan, dbDurationMs);
      spanExec?.mark("dbDurationMs", dbDurationMs);
      spanExec?.end();
    } catch (e) {
      try {
        if (state.inTx) {
          await client?.rollback();
          state.inTx = false;
        }
      } catch {
        // ignore rollback failure
      }
      writer.emit("error", evt.error(null, e));
      writer.emit("batch", evt.aborted());
      logBatchError(plan, e);
      spanExec?.mark("error", e instanceof Error ? e.message : String(e));
      spanExec?.end();
    } finally {
      try {
        await client?.end();
      } catch {
        // ignore close failure
      }
      writer.close();
    }
  })();

  // Include request id so callers can correlate logs.
  return respondWithStream(plan.responseFormat, writer.stream, {
    headers: { "x-request-id": plan.id },
  });
}
