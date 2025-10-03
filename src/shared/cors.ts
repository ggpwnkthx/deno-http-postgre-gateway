const ALLOW_ORIGIN = "*";

const ALLOW_HEADERS = [
  "authorization",
  "content-type",
  "x-db-user",
  "x-statement-timeout-ms",
  "x-statement-timeout-per-index",
  "x-lock-timeout-ms",
  "x-lock-timeout-per-index",
  "x-idle-in-txn-timeout-ms",
  "x-fetch-size",
  "x-flush-every",
  "x-flush-interval-ms",
  "x-response-format",
  "x-app-name",
  "x-allow-mutations",
  "x-perf-debug",
].join(", ");

const EXPOSE_HEADERS = "x-request-id, content-type";

const ALLOW_METHODS = "GET, POST, OPTIONS";

function corsPreflight(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": ALLOW_ORIGIN,
      "access-control-allow-headers": ALLOW_HEADERS,
      "access-control-allow-methods": ALLOW_METHODS,
      "access-control-max-age": "86400",
      "access-control-expose-headers": EXPOSE_HEADERS,
    },
  });
}

export function withCors(handler: (req: Request) => Promise<Response> | Response) {
  return async (req: Request) => {
    if (req.method === "OPTIONS") {
      return corsPreflight();
    }
    const res = await handler(req);
    const headers = new Headers(res.headers);
    headers.set("access-control-allow-origin", ALLOW_ORIGIN);
    headers.set("vary", "origin");
    headers.set("access-control-expose-headers", EXPOSE_HEADERS);
    return new Response(res.body, { status: res.status, headers });
  };
}
