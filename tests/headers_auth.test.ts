import { assertEquals, assertThrows } from "jsr:@std/assert@^1";
import { getResponseFormat } from "../src/pipeline/parse/headers.ts";
import { parseStreamingKnobs, parseTimeouts } from "../src/pipeline/parse/headers.ts";
import { parseAuth } from "../src/pipeline/parse/auth.ts";
import { HEADER } from "../src/shared/constants.ts";

Deno.test("response format: explicit header wins", () => {
  const h = new Headers({ [HEADER.RESPONSE_FORMAT]: "csv", "accept": "text/event-stream" });
  const fmt = getResponseFormat(h, h.get("accept"));
  assertEquals(fmt, "csv");
});

Deno.test("response format: Accept negotiation", () => {
  const h = new Headers({ "accept": "application/x-ndjson, text/event-stream;q=0.9" });
  const fmt = getResponseFormat(h, h.get("accept"));
  assertEquals(fmt, "ndjson");
});

Deno.test("streaming knobs parsing + clamping", () => {
  const h = new Headers({
    [HEADER.FETCH_SIZE]: "999999",
    [HEADER.FLUSH_EVERY]: "64",
    [HEADER.FLUSH_INTERVAL_MS]: "10",
  });
  const knobs = parseStreamingKnobs(h);
  assertEquals(knobs.fetchSize, 5000); // clamped to max
  assertEquals(knobs.flushEvery, 64);
  assertEquals(knobs.flushIntervalMs, 10);
});

Deno.test("timeouts: per-index maps", () => {
  const h = new Headers({
    [HEADER.STMT_TIMEOUT_PER_INDEX]: "0=100, 2=500",
    [HEADER.LOCK_TIMEOUT_PER_INDEX]: "1=250",
  });
  const t = parseTimeouts(h);
  assertEquals(t.perIndexStatement?.[0], 100);
  assertEquals(t.perIndexStatement?.[2], 500);
  assertEquals(t.perIndexLock?.[1], 250);
});

Deno.test("auth: Basic", () => {
  const token = btoa("alice:secret");
  const h = new Headers({ [HEADER.AUTHORIZATION]: `Basic ${token}` });
  const a = parseAuth(h);
  assertEquals(a.dbUser, "alice");
  assertEquals(a.dbPassword, "secret");
});

Deno.test("auth: Bearer requires x-db-user", () => {
  const h = new Headers({ [HEADER.AUTHORIZATION]: "Bearer xyz", [HEADER.DB_USER]: "bob" });
  const a = parseAuth(h);
  assertEquals(a.dbUser, "bob");
  assertEquals(a.dbPassword, "xyz");
});

Deno.test("auth: missing/invalid throws", () => {
  assertThrows(() => parseAuth(new Headers()), Error);
  const bad = new Headers({ [HEADER.AUTHORIZATION]: "Basic not-base64" });
  assertThrows(() => parseAuth(bad), Error);
});
