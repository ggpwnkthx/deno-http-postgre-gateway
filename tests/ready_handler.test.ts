import { assertEquals } from "jsr:@std/assert@^1";
import { handleReady } from "../src/pipeline/request/handlers/ready.ts";

Deno.test("ready: process-only (READY_CHECK_DB=0)", async () => {
  Deno.env.set("READY_CHECK_DB", "0");
  const res = await handleReady(new Request("http://x/ready"));
  assertEquals(res.status, 200);
  const body = await res.json();
  assertEquals(body.ok, true);
  assertEquals(!!body.db, false);
});
