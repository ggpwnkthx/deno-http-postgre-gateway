import { DEFAULTS, HEADER, type ResponseFormat } from "../../shared/constants.ts";
import { badRequest } from "../../shared/errors.ts";

/** Retrieve a header value by its canonical (hyphenated) name. Case-insensitive. */
export function getHeaderNormalized(h: Headers, name: string): string | null {
  return h.get(name);
}

/** Parse a non-negative number header, or return undefined if missing. */
function getNumberHeader(h: Headers, name: string): number | undefined {
  const v = getHeaderNormalized(h, name);
  if (!v) return undefined;
  const n = Number(v);
  if (!Number.isFinite(n) || n < 0) {
    throw badRequest(`invalid_number_header:${name}`, { value: v });
  }
  return n;
}

/** Parse a boolean-like header: true/1/yes => true, false/0/no => false. Returns undefined if missing. */
export function getBooleanHeader(h: Headers, name: string): boolean | undefined {
  const v = getHeaderNormalized(h, name);
  if (!v) return undefined;
  const s = v.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  throw badRequest(`invalid_boolean_header:${name}`, { value: v });
}

/** Parse a header like "0=100, 2=500" into a {0:100, 2:500} map. */
function getPerIndexNumberHeader(
  h: Headers,
  name: string,
): Record<number, number> | undefined {
  const v = getHeaderNormalized(h, name);
  if (!v) return undefined;
  const out: Record<number, number> = {};
  for (const part of v.split(",").map((s) => s.trim()).filter(Boolean)) {
    const [k, val] = part.split("=").map((s) => s.trim());
    const idx = Number(k), n = Number(val);
    if (!Number.isInteger(idx) || idx < 0) {
      throw badRequest(`invalid_per_index_header:${name}`, { value: part });
    }
    if (!Number.isFinite(n) || n < 0) {
      throw badRequest(`invalid_per_index_header:${name}`, { value: part });
    }
    out[idx] = n;
  }
  return out;
}

/** Determine response format from headers and Accept, with sensible defaults. */
export function getResponseFormat(h: Headers, accept: string | null): ResponseFormat {
  const explicit = getHeaderNormalized(h, HEADER.RESPONSE_FORMAT)?.toLowerCase();
  if (
    explicit === "ndjson" || explicit === "sse" || explicit === "msgpack" ||
    explicit === "cbor" || explicit === "csv" || explicit === "arrow"
  ) {
    return explicit as ResponseFormat;
  }

  if (accept) {
    // Robust Accept parsing: tokens split by "," and ";" (ignore q-values)
    const tokens = accept
      .split(",")
      .flatMap((t) => t.split(";"))
      .map((t) => t.trim().toLowerCase())
      .filter(Boolean);

    if (tokens.includes("text/event-stream")) return "sse";
    if (tokens.includes("application/x-ndjson") || tokens.includes("application/ndjson")) {
      return "ndjson";
    }
    if (tokens.includes("application/x-msgpack") || tokens.includes("application/msgpack")) {
      return "msgpack";
    }
    if (tokens.includes("application/cbor")) return "cbor";
    if (tokens.includes("text/csv") || tokens.includes("application/csv")) return "csv";
    if (tokens.includes("application/vnd.apache.arrow.stream")) return "arrow";
  }

  return DEFAULTS.RESPONSE_FORMAT;
}

export function parseStreamingKnobs(h: Headers) {
  const fsRaw = getNumberHeader(h, HEADER.FETCH_SIZE);
  const flushEvery = getNumberHeader(h, HEADER.FLUSH_EVERY) ?? DEFAULTS.FLUSH_EVERY;
  const flushIntervalMs = getNumberHeader(h, HEADER.FLUSH_INTERVAL_MS) ??
    DEFAULTS.FLUSH_INTERVAL_MS;
  let fetchSize = fsRaw ?? DEFAULTS.FETCH_SIZE;
  if (fetchSize < DEFAULTS.FETCH_MIN) fetchSize = DEFAULTS.FETCH_MIN;
  if (fetchSize > DEFAULTS.FETCH_MAX) fetchSize = DEFAULTS.FETCH_MAX;
  return { fetchSize, flushEvery, flushIntervalMs };
}

export function parseTimeouts(h: Headers) {
  return {
    statementMs: getNumberHeader(h, HEADER.STMT_TIMEOUT_MS),
    lockMs: getNumberHeader(h, HEADER.LOCK_TIMEOUT_MS),
    idleInTxnMs: getNumberHeader(h, HEADER.IDLE_IN_TXN_TIMEOUT_MS),
    perIndexStatement: getPerIndexNumberHeader(h, HEADER.STMT_TIMEOUT_PER_INDEX),
    perIndexLock: getPerIndexNumberHeader(h, HEADER.LOCK_TIMEOUT_PER_INDEX),
  };
}
