import type { Plan } from "../../../shared/types.ts";
import type { StreamEncoder } from "../../stream/encoders/types.ts";
import { seemsSelectLike } from "../sql/seems_select_like.ts";
import { evt } from "./events.ts";

/**
 * Execute a single statement at index `i`, streaming rows and/or summary as needed.
 * - SELECT-like → server-side cursor streaming via client.streamRows(...)
 * - Non-SELECT   → client.execute(...)
 */
export async function executeStatement(args: {
  client: Awaited<ReturnType<Plan["getClient"]>>;
  plan: Plan;
  writer: StreamEncoder;
  index: number;
}): Promise<void> {
  const { client, plan, writer, index } = args;
  const spec = plan.statements[index];
  const isSelect = seemsSelectLike(spec.sql);

  const span = plan.perf?.span("statement", {
    index,
    kind: isSelect ? "select_like" : "exec",
    sqlLength: spec.sql.length,
  });

  if (isSelect) {
    let rowCount = 0;
    const spanStream = plan.perf?.span("statement.stream", { index });
    const summary = await client.streamRows(
      spec.sql,
      spec.params,
      plan.fetchSize,
      (row) => {
        rowCount++;
        if (typeof row === "string" && writer.emitRow) {
          writer.emitRow(index, row);
        } else {
          writer.emit("row", evt.row(index, row as Record<string, unknown>));
        }
      },
    );
    spanStream?.mark("rowCount", rowCount);
    spanStream?.end();
    writer.emit("summary", evt.summary(index, summary));
  } else {
    const spanExec = plan.perf?.span("statement.execute", { index });
    const summary = await client.execute(spec.sql, spec.params);
    spanExec?.end();
    writer.emit("summary", evt.summary(index, summary));
  }

  span?.end();
}
