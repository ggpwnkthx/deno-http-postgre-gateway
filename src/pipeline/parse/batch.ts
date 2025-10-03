import { readJsonBody } from "../../shared/body.ts";
import { badRequest, tooMany, unprocessable } from "../../shared/errors.ts";
import type { BatchBody, StatementSpec } from "../../shared/types.ts";
import { isSingleStatement } from "../execute/sql/single_statement.ts";

export async function parseBatchRequest(
  req: Request,
  maxStatements: number,
  maxSqlLength: number,
) {
  const ct = (req.headers.get("content-type") ?? "").toLowerCase();
  if (!ct.startsWith("application/json")) throw badRequest("content_type_must_be_application_json");

  const body = await readJsonBody<BatchBody>(req, 5_000_000);
  if (!body || !Array.isArray(body.statements)) throw badRequest("invalid_body_missing_statements");
  if (body.statements.length === 0) throw badRequest("statements_must_not_be_empty");
  if (body.statements.length > maxStatements) {
    throw tooMany("too_many_statements", { max: maxStatements });
  }

  const statements: StatementSpec[] = body.statements.map((s, i) => {
    if (!s || typeof s.sql !== "string") throw badRequest("statement_missing_sql", { index: i });
    const sql = s.sql.trim();
    if (sql.length === 0) throw badRequest("empty_sql", { index: i });
    if (sql.length > maxSqlLength) throw unprocessable("sql_too_long", { index: i, maxSqlLength });
    if (!isSingleStatement(sql)) {
      throw unprocessable("multiple_statements_not_allowed", { index: i });
    }
    if (s.params && !Array.isArray(s.params)) {
      throw badRequest("params_must_be_array", { index: i });
    }
    return { sql, params: s.params ?? [] };
  });

  return { statements };
}
