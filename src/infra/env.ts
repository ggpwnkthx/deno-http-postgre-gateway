import { dotenvLoad } from "../deps.ts";
import { createPerfCtx } from "../shared/perf.ts";

let loaded = false;
let loading: Promise<void> | null = null;

/**
 * Load .env variables conditionally.
 * - Skips in production (NODE_ENV=production)
 * - Skips if DISABLE_DOTENV=true|1|yes
 * - Uses jsr:@std/dotenv load with { export: true }
 * - Ensures only one concurrent load attempt
 */
export async function loadEnv() {
  if (loaded) return;

  const disable = (Deno.env.get("DISABLE_DOTENV") ?? "").toLowerCase();
  const isDisabled = disable === "1" || disable === "true" || disable === "yes";
  const nodeEnv = (Deno.env.get("NODE_ENV") ?? "").toLowerCase();
  const isProd = nodeEnv === "production";

  if (loading) {
    await loading;
    return;
  }

  // Perf instrumentation for env load (global ctx, not per-request).
  const perf = createPerfCtx({
    enabled: (Deno.env.get("PERF_DEBUG") ?? "").trim() !== "",
    requestId: "process",
    minDurationMs: Number(Deno.env.get("PERF_MIN_MS") ?? 0),
  });

  loading = (async () => {
    const span = perf.span("env.load", { disabled: isDisabled, prod: isProd });
    try {
      if (!isDisabled && !isProd) {
        try {
          await dotenvLoad({ export: true });
          span.mark("dotenv", "loaded");
        } catch {
          // optional
          span.mark("dotenv", "skipped_or_failed");
        }
      } else {
        span.mark("dotenv", "skipped");
      }
    } finally {
      loaded = true;
      loading = null;
      span.end();
    }
  })();

  await loading;
}
