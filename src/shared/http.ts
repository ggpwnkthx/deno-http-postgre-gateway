export function json(data: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(data) + "\n", {
    status: init?.status ?? 200,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
}

export function ndjson(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "application/x-ndjson; charset=utf-8",
      "cache-control": "no-store",
      ...(init?.headers ?? {}),
    },
  });
}

export function sse(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-store",
      "x-accel-buffering": "no",
      "connection": "keep-alive",
      ...(init?.headers ?? {}),
    },
  });
}

export function msgpack(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "application/x-msgpack",
      "cache-control": "no-store",
      ...(init?.headers ?? {}),
    },
  });
}

export function cbor(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "application/cbor",
      "cache-control": "no-store",
      ...(init?.headers ?? {}),
    },
  });
}

export function csv(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "text/csv; charset=utf-8",
      "cache-control": "no-store",
      ...(init?.headers ?? {}),
    },
  });
}

export function arrowIpcStream(stream: ReadableStream<Uint8Array>, init?: ResponseInit) {
  return new Response(stream, {
    status: init?.status ?? 200,
    headers: {
      "content-type": "application/vnd.apache.arrow.stream",
      "cache-control": "no-store",
      ...(init?.headers ?? {}),
    },
  });
}

export const notFound = () => json({ error: "not_found" }, { status: 404 });
export const internalError = (details?: unknown) =>
  json({ error: "internal_error", details }, { status: 500 });

export function methodNotAllowed(allow: string[]) {
  return json({ error: "method_not_allowed", allow }, {
    status: 405,
    headers: { "allow": allow.join(", ") },
  });
}
