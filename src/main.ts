import { loadEnv } from "./infra/env.ts";
import { router } from "./pipeline/request/router.ts";
import { withCors } from "./shared/cors.ts";
import { internalError, json } from "./shared/http.ts";

function extractErrorCode(reason: unknown): string | number | null {
  if (typeof reason === "object" && reason !== null) {
    const obj = reason as Record<string, unknown>;
    const direct = obj.code;
    const fields = obj.fields as Record<string, unknown> | undefined;
    const fieldCode = fields?.code;
    if (typeof direct === "string" || typeof direct === "number") return direct;
    if (typeof fieldCode === "string" || typeof fieldCode === "number") return fieldCode;
  }
  return null;
}

// --- Defensive: keep the process alive on stray async rejections and log them clearly.
globalThis.addEventListener("unhandledrejection", (evt) => {
  try {
    const reason = evt.reason;
    const payload = {
      level: "error",
      event: "unhandled_rejection",
      message: reason instanceof Error ? reason.message : String(reason),
      code: extractErrorCode(reason),
      stack: reason instanceof Error ? reason.stack ?? null : null,
    };
    console.error(JSON.stringify(payload));
  } catch (_logErr) {
    console.error("[unhandledrejection]", evt.reason);
  } finally {
    // Prevent process crash on unhandled rejections (esp. during driver startup paths).
    evt.preventDefault();
  }
});

globalThis.addEventListener("error", (evt) => {
  try {
    const payload = {
      level: "error",
      event: "uncaught_exception",
      message: evt.error instanceof Error ? evt.error.message : String(evt.message),
      stack: evt.error instanceof Error ? evt.error.stack ?? null : null,
    };
    console.error(JSON.stringify(payload));
  } catch {
    console.error("[uncaught_exception]", evt.message);
  } finally {
    // Avoid crashing the server; convert to logged error.
    evt.preventDefault();
  }
});

await loadEnv();
const PORT = Number(Deno.env.get("PORT") ?? "8000");

Deno.serve(
  {
    port: PORT,
    onError: (e: unknown) => {
      const message = e instanceof Error ? e.message : typeof e === "string" ? e : (() => {
        try {
          return JSON.stringify(e);
        } catch {
          return String(e);
        }
      })();
      console.error("[onError]", e);
      return internalError({ message });
    },
  },
  withCors((req) => {
    const { pathname } = new URL(req.url);
    if (req.method === "GET" && pathname === "/healthz") return json({ ok: true });
    return router(req);
  }),
);
