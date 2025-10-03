import { createPerfCtx } from "../src/shared/perf.ts";
import { assertEquals } from "jsr:@std/assert@^1";

Deno.test("perf ctx disabled → no-op span", () => {
  const perf = createPerfCtx({ enabled: false });
  const s = perf.span("x");
  s.mark("a", 1);
  s.incr("b");
  s.end();
  assertEquals(perf.enabled, false);
});

Deno.test("perf ctx enabled → span logs (not asserting output, just smoke)", () => {
  const perf = createPerfCtx({ enabled: true, sampleRate: 1, minDurationMs: 0 });
  const s = perf.span("sample", { foo: 1 });
  s.incr("count", 2);
  s.mark("bar", "baz");
  s.end();
  // If it didn't throw, we're good.
  assertEquals(perf.enabled, true);
});
