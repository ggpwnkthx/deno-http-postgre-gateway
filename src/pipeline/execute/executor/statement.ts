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

    // Choose the most efficient delivery form supported by the encoder.
    const wantsArrays = writer.preferArrays === true;
    const wantsObjects = writer.preferObjects === true && !wantsArrays; // arrays win

    // We'll capture schema if provided so we can fallback safely if needed.
    let cols: string[] | null = null;

    const summary = await client.streamRows(
      spec.sql,
      spec.params,
      plan.fetchSize,
      (row) => {
        rowCount++;
        // Fast path for text encoders (NDJSON/SSE): they get pre-serialized strings.
        if (!wantsArrays && !wantsObjects && typeof row === "string" && writer.emitRow) {
          writer.emitRow(index, row);
          return;
        }

        // Arrays preferred path
        if (wantsArrays && Array.isArray(row)) {
          if (typeof writer.emitArrayRow === "function") {
            writer.emitArrayRow(index, row as unknown[]);
            return;
          }
          // Fallback: map to object if encoder didn't implement emitArrayRow
          if (cols) {
            const obj: Record<string, unknown> = {};
            for (let i = 0; i < cols.length; i++) obj[cols[i]] = (row as unknown[])[i];
            writer.emit("row", evt.row(index, obj));
            return;
          }
        }

        // Object mode (native objects)
        if (wantsObjects && row && typeof row === "object" && !Array.isArray(row)) {
          writer.emit("row", evt.row(index, row as Record<string, unknown>));
          return;
        }

        // Last-resort compatibility: stringify unknowns
        writer.emit("row", evt.row(index, { value: row as unknown }));
      },
      {
        as: wantsArrays ? "array" : (wantsObjects ? "object" : "json"),
        onSchema: (c) => {
          cols = c;
          try {
            writer.onSchema?.(index, c);
          } catch {
            // ignore encoder schema hook errors
          }
        },
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
