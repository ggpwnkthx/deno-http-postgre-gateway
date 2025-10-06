import { assert, assertEquals, assertFalse } from "jsr:@std/assert@^1";
import { isSingleStatement } from "../src/pipeline/execute/sql/single_statement.ts";
import { firstTopLevelCommand } from "../src/pipeline/execute/sql/first_command.ts";
import { detectPgBouncerUnsafe } from "../src/pipeline/execute/sql/pgbouncer.ts";

Deno.test("single-statement: simple", () => {
  assert(isSingleStatement("select 1"));
  assert(isSingleStatement("/*x*/ select 1 -- tail"));
});

Deno.test("single-statement: rejects multi-statement", () => {
  assertFalse(isSingleStatement("select 1; select 2"));
});

Deno.test("single-statement: semicolon inside dollar-quoted body ignored", () => {
  const sql = "select $$a;b;c$$ as t";
  assert(isSingleStatement(sql));
});

Deno.test("firstTopLevelCommand: skips with/cte", () => {
  const sql = `
    with t as (select 1 as x)
    select * from t
  `;
  assertEquals(firstTopLevelCommand(sql), "select");
});

Deno.test("PgBouncer unsafe detection", () => {
  assertEquals(detectPgBouncerUnsafe("SET search_path = public"), ["set"]);
  assertEquals(detectPgBouncerUnsafe("prepare transaction 'x'"), ["prepare transaction"]);
  assertEquals(detectPgBouncerUnsafe("LISTEN foo"), ["listen"]);
  assertEquals(detectPgBouncerUnsafe("select 1"), []);
});
