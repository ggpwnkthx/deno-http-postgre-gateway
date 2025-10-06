import { assert, assertEquals } from "jsr:@std/assert@^1";
import { buildTls } from "../src/pipeline/execute/db/tls.ts";
import { jitt, withTimeout } from "../src/pipeline/execute/db/utils.ts";
import { classifyConnectError } from "../src/pipeline/execute/db/errors.ts";

Deno.test("buildTls: modes", () => {
  assertEquals(buildTls("disable"), undefined);
  assertEquals(buildTls("require"), { enforce: true });
});

Deno.test("buildTls: verify-full requires cert", () => {
  let threw = false;
  try {
    buildTls("verify-full");
  } catch {
    threw = true;
  }
  assert(threw);
  assertEquals(buildTls("verify-full", "CA_CERT_PEM"), {
    enforce: true,
    caCertificates: ["CA_CERT_PEM"],
  });
});

Deno.test("jitt produces within bounds", () => {
  for (let i = 0; i < 10; i++) {
    const v = jitt(5, 10);
    assert(v >= 5 && v <= 10);
  }
});

Deno.test("withTimeout rejects on timeout", async () => {
  const slow = new Promise((resolve) => setTimeout(resolve, 50));
  let timedOut = false;
  try {
    await withTimeout(slow, 10, "x");
  } catch {
    timedOut = true;
  }
  assert(timedOut);
});

Deno.test("classifyConnectError: auth and availability", () => {
  const authErr = classifyConnectError({
    code: "28P01",
    message: "password authentication failed",
  });
  assertEquals(authErr.status, 401);

  const unavail = classifyConnectError({ message: "connection timed out" });
  assertEquals(unavail.status, 503);

  const other = classifyConnectError({ message: "weird" });
  assertEquals(other.status, 500);
});
