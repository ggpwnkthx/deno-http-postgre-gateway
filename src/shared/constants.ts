export const DEFAULTS = {
  FETCH_SIZE: 500,
  FETCH_MIN: 50,
  FETCH_MAX: 5000,
  FLUSH_EVERY: 0,
  FLUSH_INTERVAL_MS: 0,
  MAX_STATEMENTS: 50,
  MAX_SQL_LENGTH: 100_000,
  RESPONSE_FORMAT: "ndjson" as const,
};

export const HEADER = {
  AUTHORIZATION: "authorization",
  DB_USER: "x-db-user",
  FETCH_SIZE: "x-fetch-size",
  FLUSH_EVERY: "x-flush-every",
  FLUSH_INTERVAL_MS: "x-flush-interval-ms",
  RESPONSE_FORMAT: "x-response-format",
  APP_NAME: "x-app-name",
  STMT_TIMEOUT_MS: "x-statement-timeout-ms",
  LOCK_TIMEOUT_MS: "x-lock-timeout-ms",
  IDLE_IN_TXN_TIMEOUT_MS: "x-idle-in-txn-timeout-ms",
  STMT_TIMEOUT_PER_INDEX: "x-statement-timeout-per-index",
  LOCK_TIMEOUT_PER_INDEX: "x-lock-timeout-per-index",
  ALLOW_MUTATIONS: "x-allow-mutations",
  /** Per-request performance debugging toggle */
  PERF_DEBUG: "x-perf-debug",
} as const;

export type ResponseFormat =
  | "ndjson"
  | "sse"
  | "msgpack"
  | "cbor"
  | "csv"
  | "arrow";
