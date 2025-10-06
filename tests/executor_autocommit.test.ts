import { assert, assertEquals } from "jsr:@std/assert@^1";
import { executePlan } from "../src/pipeline/execute/executor/runner.ts";
import { DEFAULTS } from "../src/shared/constants.ts";
import type { Plan } from "../src/shared/types.ts";
import { MockDbClient } from "./helpers/mock_db_client.ts";

function planFor(
  statements: string[],
  overrides: Partial<Plan> = {},
  mock?: MockDbClient,
): Plan {
  const client = mock ?? new MockDbClient({
    rowSets: { "select 1 as n": ['{"n":1}'] },
  });
  return {
    id: crypto.randomUUID(),
    dbUser: "u",
    dbHost: "h",
    dbName: "d",
    fetchSize: DEFAULTS.FETCH_SIZE,
    flushEvery: 0,
    flushIntervalMs: 0,
    responseFormat: "ndjson",
    timeouts: {},
    statements: statements.map((sql) => ({ sql, params: [] })),
    getClient: () => Promise.resolve(client),
    allowMutations: true,
    readOnlyBatch: statements.every((s) => /^\s*(select|values|table|show|explain)/i.test(s)),
    autocommitReadOnly: true,
    ...overrides,
  };
}

Deno.test("executor: read-only batch uses autocommit per statement", async () => {
  const mock = new MockDbClient({
    rowSets: { "select 1 as n": ['{"n":1}'], "select 2 as n": ['{"n":2}'] },
  });
  const plan = planFor(["select 1 as n", "select 2 as n"], {}, mock);
  const res = executePlan(plan);
  // fully drain stream
  await res.body?.cancel();
  // Autocommit path should BEGIN/COMMIT for each statement, not one big txn.
  const beginCount = mock.calls.filter((c) => c.op === "BEGIN").length;
  const commitCount = mock.calls.filter((c) => c.op === "COMMIT").length;
  assertEquals(beginCount, 2);
  assertEquals(commitCount, 2);
});

Deno.test("executor: single txn for mixed statements", async () => {
  const mock = new MockDbClient({
    summaries: { "insert into t values (1)": { command: "INSERT", rowCount: 1 } },
  });
  const plan = planFor(
    ["select 1 as n", "insert into t values (1)"],
    { readOnlyBatch: false },
    mock,
  );
  const res = executePlan(plan);
  await res.body?.cancel();
  const calls = mock.calls.map((c) => c.op);
  // Expect single BEGIN ... COMMIT
  const beginIdx = calls.indexOf("BEGIN");
  const commitIdx = calls.lastIndexOf("COMMIT");
  assert(beginIdx !== -1 && commitIdx !== -1 && beginIdx < commitIdx);
});
