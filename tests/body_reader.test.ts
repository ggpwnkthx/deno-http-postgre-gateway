import { assertEquals, assertRejects } from "jsr:@std/assert@^1";
import { readJsonBody } from "../src/shared/body.ts";

function reqWithBody(obj: unknown, contentLength?: number): Request {
  const bytes = new TextEncoder().encode(JSON.stringify(obj));
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      c.enqueue(bytes);
      c.close();
    },
  });
  const headers = new Headers({ "content-type": "application/json" });
  if (typeof contentLength === "number") {
    headers.set("content-length", String(contentLength));
  }
  return new Request("http://x", { method: "POST", headers, body });
}

Deno.test("readJsonBody: exact content-length", async () => {
  const obj = { a: 1 };
  const r = reqWithBody(obj, JSON.stringify(obj).length);
  const parsed = await readJsonBody<typeof obj>(r, 1024);
  assertEquals(parsed, obj);
});

Deno.test("readJsonBody: too large (via content-length)", async () => {
  const obj = { a: 1 };
  const r = reqWithBody(obj, 10_000);
  await assertRejects(() => readJsonBody(r, 1024));
});

Deno.test("readJsonBody: no content-length, enforce cap", async () => {
  const obj = { big: "x".repeat(2048) };
  const r = reqWithBody(obj);
  await assertRejects(() => readJsonBody(r, 1024));
});
