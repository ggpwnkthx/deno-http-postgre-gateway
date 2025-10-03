
# Deno Postgres Streaming Gateway

A lightweight Deno v2.5 HTTP service that turns incoming requests into a **request → parse → plan → execute → stream** pipeline for PostgreSQL.  
It supports **NDJSON** and **Server-Sent Events (SSE)** streaming with row-by-row backpressure-friendly delivery, per-statement timeouts, and simple Basic/Bearer auth.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Folder Structure](#folder-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Environment](#environment)
  - [Auth](#auth)
  - [Headers](#headers)
  - [Streaming Formats](#streaming-formats)
- [API](#api)
  - [POST /batch](#post-batch)
  - [Health](#health)
- [Examples](#examples)
  - [NDJSON](#ndjson)
  - [SSE](#sse)
  - [Basic Auth](#basic-auth)
  - [Bearer + X-DB-User](#bearer--x-db-user)
- [Error Model](#error-model)
- [CORS](#cors)
- [Development](#development)
- [Performance Tips](#performance-tips)
- [Notes](#notes)

---

## Features

- **Streaming** results as **NDJSON** or **SSE** (row-by-row for SELECT-like queries).
- **Single-statement enforcement** per item (guards against `;` injections).
- **Per-statement timeouts** (statement/lock) with global + per-index overrides.
- **Simple auth**: Basic (`user:password`) or Bearer (`password` + `x-db-user`).
- **Backpressure-aware** line buffering with configurable flush interval and batch size.
- **Postgres server-side cursors** under the hood for scalable streaming reads.
- **Autocommit optimization for read-only batches**: when enabled (default), each SELECT-like statement runs in its **own tiny transaction** for PgBouncer friendliness.
- **Consistent connect error classification** into `401` (auth), `503` (unavailable), or `500` (internal).

---

## Architecture

This service organizes all logic into a clear pipeline:

1. **request** → incoming HTTP route selection.
2. **parse** → headers, auth, and body validation transformed into typed data.
3. **plan** → resolve environment + build a `getClient` factory with auth role.
4. **execute** → run the plan in a transaction or per-statement autocommit mode, stream rows/summaries, handle timeouts.
5. **stream** → encode to **NDJSON** or **SSE** and write the HTTP response.

---

## Quick Start

### 1) Requirements
- **Deno v2.5+**
- A reachable **PostgreSQL** instance

### 2) Configure environment

Create a `.env` (optional — values can also be exported in your shell):

```env
# required
PGHOST=localhost
PGDATABASE=postgres

# optional
PGPORT=5432
PGSSLMODE=require          # one of: disable | require | verify-full
PGSSLROOTCERT=             # required if PGSSLMODE=verify-full

# server
PORT=8000
```

### 3) Run the server

```bash
deno run -A src/main.ts
# or, with explicit perms (adjust as needed)
# deno run --allow-env --allow-net --allow-read src/main.ts
```

The server listens on `http://localhost:${PORT}` (defaults to `8000`).

---

## Configuration

### Environment

- `PGHOST` (required) – Postgres host  
- `PGDATABASE` (required) – Postgres database name  
- `PGPORT` (optional, default `5432`)  
- `PGSSLMODE` (optional, default `require`) – `disable | require | verify-full`  
- `PGSSLROOTCERT` (required when `PGSSLMODE=verify-full`)  
- `PORT` (optional, default `8000`)

**Connection tuning (optional):**

- `PG_CONNECT_ATTEMPTS` (default `3`)
- `PG_CONNECT_MIN_BACKOFF_MS` (default `50`)
- `PG_CONNECT_MAX_BACKOFF_MS` (default `250`)
- `PG_CONNECT_TIMEOUT_MS` (default `3000`)

**Behavioral flags:**

- `READ_ONLY` (default `false`) – When `true`, mutations are disallowed unless explicitly permitted per request via header (see below).
- `PGBOUNCER_MODE` (optional) – `"transaction"` or `"statement"` automatically enables PgBouncer strict checks; can be overridden by `PGBOUNCER_STRICT`.
- `PGBOUNCER_STRICT` (default depends on `PGBOUNCER_MODE`) – Rejects session-unsafe statements (`SET`, `LISTEN`, `COPY`, `BEGIN/COMMIT`, etc.).
- `AUTOCOMMIT_READONLY` (default `true`) – Run **read-only batches** as **autocommit**: each statement in its own small transaction.
- `READY_CHECK_DB` (default `false`) – If `true`, `/ready` probes DB using `PGREADY_USER`/`PGREADY_PASSWORD`.
- `PGREADY_USER`, `PGREADY_PASSWORD` – Credentials used only for `/ready` probe.

### Auth

Two auth schemes are supported:

1) **Basic**  
   `Authorization: Basic base64("dbUser:dbPassword")`

2) **Bearer**  
   `Authorization: Bearer <dbPassword>` with **`x-db-user`** header set to the user.

If auth is missing or malformed, a `401` is returned with a consistent JSON error.

### Headers

The following custom headers control behavior (all **lowercase** on the wire):

| Header | Type | Description |
|---|---|---|
| `authorization` | string | `Basic` or `Bearer` (see above). |
| `x-db-user` | string | Required only for Bearer auth. |
| `x-app-name` | string | Optional application name for PG `application_name`. |
| `x-response-format` | enum | `"ndjson"` or `"sse"`. If omitted, uses defaults or `Accept: text/event-stream` to infer SSE. |
| `x-fetch-size` | number | Cursor fetch size per round trip. Default **500**, min **50**, max **5000**. |
| `x-flush-every` | number | Buffer size (# of lines) before flushing. `0` means flush each line immediately. Default **0**. |
| `x-flush-interval-ms` | number | Time-based flush when buffering; `0` disables timer. Default **0**. |
| `x-statement-timeout-ms` | number | Global `statement_timeout` in ms. |
| `x-lock-timeout-ms` | number | Global `lock_timeout` in ms. |
| `x-idle-in-txn-timeout-ms` | number | Global `idle_in_transaction_session_timeout` in ms. |
| `x-statement-timeout-per-index` | map | Per-statement override, e.g. `"0=100,2=500"`. |
| `x-lock-timeout-per-index` | map | Per-statement override, e.g. `"1=250"`. |
| `x-allow-mutations` | bool | Overrides read-only mode when `READ_ONLY=1`. |

### Streaming Formats

- **NDJSON**: each line is a JSON object. Event *type* is not included; the structure itself conveys the event.
- **SSE**: uses `event: <type>` and `data: <json>` frames. A `:ok` comment is sent on connect.

Choose explicitly via `x-response-format`, or set `Accept: text/event-stream` to get SSE by default.

---

## API

### POST `/batch`

Submit multiple single SQL statements to run **either**:

- in a **single transaction** (default), **or**
- in **autocommit** mode (for **read-only batches** when `AUTOCOMMIT_READONLY=true`), where each SELECT-like statement runs in its own short transaction.

**Request Body**

```json
{
  "statements": [
    { "sql": "SELECT 1 as n" },
    { "sql": "SELECT * FROM users WHERE id = $1", "params": [42] }
  ]
}
```

- Each `statements[i].sql` must be a **single** statement (no unescaped `;`).
- `params` (optional) must be an array when present.
- Limits (defaults):
  - `MAX_STATEMENTS` = **50**
  - `MAX_SQL_LENGTH` = **100000**

**Behavior**
- A transaction is started (`BEGIN`), statements are executed in order, then committed.  
  If the batch is **read-only** and `AUTOCOMMIT_READONLY=true`, each statement is executed inside its **own transaction** (cursor-safe for PgBouncer).
- If any step fails:
  - In single-transaction mode → the transaction is rolled back, an error is streamed, and the batch is marked aborted.
  - In autocommit mode → only the failing statement’s transaction is rolled back; the batch streams an error and abort event.

**Stream Payloads**

- **NDJSON** lines (examples):
  ```json
  {"batch":{"begin":true,"statements":2,"requestId":"..."}}
  {"statement":0,"row":{"n":1}}
  {"statement":0,"summary":{"command":"SELECT","rowCount":null}}
  {"statement":1,"summary":{"command":"SELECT","rowCount":null}}
  {"batch":{"committed":true,"dbDurationMs":17}}
  ```

- **SSE** frames (examples):
  ```text
  :ok

  event: batch
  data: {"batch":{"begin":true,"statements":2,"requestId":"..."}}

  event: row
  data: {"statement":0,"row":{"n":1}}

  event: summary
  data: {"statement":0,"summary":{"command":"SELECT","rowCount":null}}

  event: summary
  data: {"statement":1,"summary":{"command":"SELECT","rowCount":null}}

  event: batch
  data: {"batch":{"committed":true,"dbDurationMs":17}}
  ```

### Health

- `GET /healthz` → `{ "ok": true }`
- `GET /ready` → process readiness; optionally includes DB probe results when `READY_CHECK_DB=1` and probe credentials provided.

---

## Examples

> Replace credentials, host, and SQL as needed.

### NDJSON

```bash
curl -sN   -H "authorization: Basic $(printf 'alice:secret' | base64)"   -H "content-type: application/json"   -H "x-response-format: ndjson"   -X POST http://localhost:8000/batch   --data '{"statements":[{"sql":"select 1 as n"}]}'
```

### SSE

```bash
curl -sN   -H "authorization: Basic $(printf 'alice:secret' | base64)"   -H "content-type: application/json"   -H "accept: text/event-stream"   -X POST http://localhost:8000/batch   --data '{"statements":[{"sql":"select generate_series(1,5) as n"}]}'
```

### Basic Auth

```bash
# Authorization: Basic base64("dbUser:dbPassword")
curl -sN   -H "authorization: Basic $(printf 'reporter:reporter_password' | base64)"   -H "content-type: application/json"   -X POST http://localhost:8000/batch   --data '{"statements":[{"sql":"select current_user"}]}'
```

### Bearer + X-DB-User

```bash
# Authorization: Bearer <dbPassword> and x-db-user: <dbUser>
curl -sN   -H "authorization: Bearer reporter_password"   -H "x-db-user: reporter"   -H "content-type: application/json"   -X POST http://localhost:8000/batch   --data '{"statements":[{"sql":"select current_user"}]}'
```

---

## Error Model

Errors are normalized to a consistent JSON shape. Typical status codes:

- **400** `bad_request` – invalid headers/body/params
- **401** `unauthorized` – missing/invalid auth (or connect-time auth failure)
- **422** `unprocessable` – e.g., multi-statement SQL detected, or SQL too long
- **429** `too_many` – exceeds `MAX_STATEMENTS`
- **503** `db_unavailable` – connect-time unavailability (timeouts/refused)
- **500** `internal_error` – uncaught/server errors

**Shape**

```json
{
  "error": "bad_request",
  "message": "invalid_per_index_header:x-statement-timeout-per-index",
  "details": { "value": "abc=xyz" }
}
```

For `401` with missing credentials, you may receive a helpful `hint`:

```json
{
  "error": "unauthorized",
  "message": "missing_authorization_header",
  "details": null,
  "hint": "Provide Authorization header (Basic or Bearer + X-DB-User)"
}
```

---

## CORS

- Allowed origins: `*`
- Allowed methods: `GET, POST, OPTIONS`
- Allowed headers:  
  `authorization, content-type, x-db-user, x-statement-timeout-ms, x-statement-timeout-per-index, x-lock-timeout-ms, x-lock-timeout-per-index, x-idle-in-txn-timeout-ms, x-fetch-size, x-flush-every, x-flush-interval-ms, x-response-format, x-app-name, x-allow-mutations`

Preflight responses return `204` with a max-age of 86400 seconds.

---

## Development

- `src/shared/env.ts` provides `envBool` used across handlers.
- Connect-time errors are classified in `src/pipeline/execute/db/errors.ts` and respected both in retry logic and the `/batch` pre-connect path.
- `StreamEncoder` type is defined in `src/pipeline/stream/encoders/types.ts` and shared by NDJSON/SSE implementations.
- Modify defaults in `src/shared/constants.ts`.
- DB adapter is in `src/pipeline/execute/db/adapter.ts` (Postgres).

Run locally:

```bash
deno run -A src/main.ts
# visit: http://localhost:8000/healthz
```

---

## Performance Tips

- **`x-fetch-size`**: larger values reduce round trips but increase memory per chunk.
- **`x-flush-every`**: set to `0` for lowest latency; set higher to batch encoder writes.
- **`x-flush-interval-ms`**: pair with a nonzero buffer to bound latency under low row rates.
- **Per-index timeouts**: e.g., make a long-running statement tolerant without penalizing others:  
  `x-statement-timeout-per-index: 0=200,2=5000`

---

## Notes

- **Transaction model**
  - By default, the executor wraps the batch in a single transaction:
    - Success → `COMMIT` and `{"batch":{"committed":true,...}}`
    - Failure → `ROLLBACK` and `{"batch":{"aborted":true}}` with an `error` event.
  - When `AUTOCOMMIT_READONLY=true` *and* the batch is **read-only**, each statement runs in its **own** small transaction (cursor-friendly for PgBouncer).

- **Statement classification**
  - SELECT-like detection controls streaming (`SELECT`, `WITH`, `SHOW`, `VALUES`, `EXPLAIN`).
  - PgBouncer-unsafe commands are rejected when strict mode is enabled.

- **Type surface**
  - Internal-only JSON helper types and guards that weren’t used have been removed for a smaller API surface.
