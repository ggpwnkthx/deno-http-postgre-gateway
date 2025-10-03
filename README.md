# Deno Postgres Streaming Gateway

A tiny, production-ready HTTP gateway for Deno v2.5+ that turns batched SQL into backpressure-aware, row-by-row streams from PostgreSQL. It speaks six wire formats (NDJSON, SSE, MessagePack, CBOR, CSV, and Apache Arrow IPC) so browsers, CLIs, and data pipelines can all consume results efficiently. Built on server-side cursors with per-statement timeouts, autocommit for read-only batches, and PgBouncer-friendly behavior, it pairs simple Basic/Bearer auth with predictable error handling and minimal latency.

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
    - [NDJSON](#ndjson)
    - [SSE](#sse)
    - [MessagePack](#messagepack)
    - [CBOR](#cbor)
    - [CSV](#csv)
    - [Apache Arrow IPC](#apache-arrow-ipc)
  - [Binary Values \u0026 Types](#binary-values--types)
- [API](#api)
  - [POST /batch](#post-batch)
  - [Health](#health)
- [Examples](#examples)
  - [NDJSON](#examples-ndjson)
  - [SSE](#examples-sse)
  - [MessagePack](#examples-messagepack)
  - [CBOR](#examples-cbor)
  - [CSV](#examples-csv)
  - [Apache Arrow IPC](#examples-apache-arrow-ipc)
- [Error Model](#error-model)
- [CORS](#cors)
- [Development](#development)
- [Performance Tips](#performance-tips)
- [Benchmarks \u0026 Tuning](#benchmarks--tuning)
- [Notes](#notes)

---

## Features

- **Streaming** results as **NDJSON**, **SSE**, **MessagePack**, **CBOR**, **CSV**, or **Apache Arrow IPC**.
- **Row-by-row** delivery for SELECT-like queries using server-side cursors (scales to large result sets).
- **Single-statement enforcement** (guards against `;` injections).
- **Per-statement timeouts** (global defaults + per-index overrides).
- **Simple auth**: Basic (`user:password`) or Bearer (`password` + `x-db-user`).
- **Backpressure-aware** buffering with configurable **flush interval** and **batch size**.
- **Autocommit optimization for read-only batches** (PgBouncer‑friendly): each SELECT-like statement runs in its **own tiny transaction**.
- **Consistent connect error classification** into `401` (auth), `503` (unavailable), or `500` (internal).
- **Optional perf logs** (`x-perf-debug` / `PERF_DEBUG`) with structured span markers.

---

## Architecture

This service organizes logic into a clear pipeline:

1. **request** → incoming HTTP route selection.  
2. **parse** → headers, auth, and body validation transformed into typed data.  
3. **plan** → resolve environment + build a `getClient` factory with auth role.  
4. **execute** → run the plan (single transaction or autocommit for read-only), stream rows/summaries, handle timeouts, and cancellation.  
5. **stream** → encode to the chosen format and return the HTTP response.

---

## Folder Structure

```
src/
  infra/                 # env loader + perf bootstrap
  pipeline/
    execute/             # DB connect, driver helpers, executor
      db/                # Postgres adapter + connect error classification
      executor/          # batch runner, statement execution, tx mgmt
      sql/               # lightweight SQL scanners (first command, etc.)
    parse/               # headers/auth/body parsing
    request/             # router + handlers
    stream/
      encoder.ts         # format factory + HTTP response helpers
      encoders/          # ndjson, sse, msgpack, cbor, csv, arrow
  shared/                # constants, errors, http helpers, perf
bench/
  stream_formats.bench.ts
```

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

- `READ_ONLY` (default `false`) – When `true`, mutations are disallowed unless explicitly permitted per request via header.  
- `PGBOUNCER_MODE` (optional) – `"transaction"` or `"statement"` automatically enables PgBouncer strict checks; can be overridden by `PGBOUNCER_STRICT`.  
- `PGBOUNCER_STRICT` (default depends on `PGBOUNCER_MODE`) – Rejects session-unsafe statements (`SET`, `LISTEN`, `COPY`, `BEGIN/COMMIT`, etc.).  
- `AUTOCOMMIT_READONLY` (default `true`) – Run **read-only batches** as **autocommit** (one short txn per statement).  
- `READY_CHECK_DB` (default `false`) – If `true`, `/ready` probes DB using `PGREADY_USER`/`PGREADY_PASSWORD`.

### Auth

Two auth schemes are supported:

1) **Basic** – `Authorization: Basic base64("dbUser:dbPassword")`  
2) **Bearer** – `Authorization: Bearer <dbPassword>` together with **`x-db-user: <dbUser>`**

### Headers

All custom headers use **lowercase** on the wire:

| Header | Type | Description |
|---|---|---|
| `authorization` | string | `Basic` or `Bearer` (see above). |
| `x-db-user` | string | Required only for Bearer auth. |
| `x-app-name` | string | Optional application name for PG `application_name`. |
| `x-response-format` | enum | `ndjson | sse | msgpack | cbor | csv | arrow`. |
| `x-fetch-size` | number | Cursor fetch size per round trip. Default **500**, min **50**, max **5000**. |
| `x-flush-every` | number | Buffer size before flushing. `0` = flush each record immediately. Default **0**. |
| `x-flush-interval-ms` | number | Time-based flush; `0` disables timer. Default **0**. |
| `x-statement-timeout-ms` | number | Global `statement_timeout` in ms. |
| `x-lock-timeout-ms` | number | Global `lock_timeout` in ms. |
| `x-idle-in-txn-timeout-ms` | number | Global `idle_in_transaction_session_timeout` in ms. |
| `x-statement-timeout-per-index` | map | Per-statement override, e.g. `"0=100,2=500"`. |
| `x-lock-timeout-per-index` | map | Per-statement override, e.g. `"1=250"`. |
| `x-allow-mutations` | bool | Overrides read-only mode when `READ_ONLY=1`. |
| `x-perf-debug` | bool | Enables per-request perf logging. |

---

## Streaming Formats

You can select a format either by sending `x-response-format` or by using an appropriate `Accept:` header.  
If both are provided, `x-response-format` wins.

### NDJSON

- **Content-Type:** `application/x-ndjson; charset=utf-8`  
- **Accept triggers:** `application/x-ndjson`, `application/ndjson`  
- **Wire shape:** line-delimited JSON objects. Row events look like  
  `{"statement":<i>,"row":{...}}`. Other events are self-describing objects, e.g.  
  `{"batch":{"begin":true,...}}`, `{"statement":0,"summary":{"command":"SELECT","rowCount":null}}`.

**When to use:** human-readable streaming, easy to debug and pipe via CLI tools.

### SSE

- **Content-Type:** `text/event-stream; charset=utf-8`  
- **Accept triggers:** `text/event-stream`  
- **Wire shape:** [SSE frames](https://html.spec.whatwg.org/multipage/server-sent-events.html). We send a `:ok` comment on connect. Rows use the `row` event:
  ```
  event: row
  data: {"statement":0,"row":{"n":1}}
  ```
  Other events set `event: <type>` with `data: <json>`.

**When to use:** browsers, EventSource-compatible clients.

### MessagePack

- **Content-Type:** `application/x-msgpack`  
- **Accept triggers:** `application/x-msgpack`, `application/msgpack`  
- **Wire shape:** concatenated MessagePack **documents**.  
  - **Rows** are **batched** into documents of the form:  
    `{ rows: [ { statement: number, row: <object> }, ... ] }`  
  - **Non-row events** (batch begin/commit, summary, error) are emitted as their own MsgPack documents (using the same JSON shapes as NDJSON).  
- **Binary fields:** byte arrays are **typed** (`Uint8Array`) in the encoded payload (not `"base64:..."`).

**When to use:** compact binary streaming; lower CPU and bandwidth than JSON.

### CBOR

- **Content-Type:** `application/cbor`  
- **Accept triggers:** `application/cbor`  
- **Wire shape:** concatenated CBOR **documents** using the same batching policy as MessagePack:  
  `{ rows: [ { statement, row }, ... ] }` for rows; non-row events as standalone documents.  
- **Binary fields:** encoded as native byte strings; no base64 wrapper.

**When to use:** compact binary with rich type support and broad tooling.

### CSV

- **Content-Type:** `text/csv; charset=utf-8` (line-based)  
- **Accept triggers:** `text/csv`, `application/csv`  
- **Wire shape:** comment-prefixed metadata + per-statement CSV sections.
  - Non-row events are written as **comment** lines beginning with `# `, e.g. `# {"batch":{"begin":true,...}}`
  - For each statement index, the encoder emits a **section header** on first row: `# statement:<i>` followed by a **header row** inferred from the first row object’s keys.
  - **Cells:** scalars are written plainly; complex values are **JSON-stringified**; binary becomes `"base64:..."` strings.

**When to use:** interoperability with spreadsheets/CSV tooling; simple piping to other systems.

### Apache Arrow IPC

- **Content-Type:** `application/vnd.apache.arrow.stream`  
- **Accept triggers:** `application/vnd.apache.arrow.stream`  
- **Wire shape:** **concatenated Arrow IPC streams** (one or more per statement).  
  - Rows are **accumulated per statement** and flushed in **record batches**.  
  - Batch size is derived from `x-flush-every` (if > 0) else defaults to **2048** rows.  
  - On statement `summary`, we flush any remaining rows.  
  - Consumers must be able to read **multiple IPC streams back-to-back**.
  
**When to use:** columnar analytics; zero-copy reads in Arrow-enabled runtimes.

---

### Binary Values & Types

The gateway normalizes some JS types to ensure consistent wire representations:

- **BigInt** → string (e.g. `"9007199254740993"`)
- **Date** → ISO string (UTC) via `.toISOString()`
- **Uint8Array/ArrayBuffer** → `"base64:..."` **only in JSON/CSV** encodings (NDJSON/SSE/CSV)  
  - **MessagePack/CBOR**: binary is decoded back into `Uint8Array` (typed bytes).  
  - **Arrow**: binary is part of the JSON-to-Table conversion; use appropriate Arrow field types client‑side.

---

## API

### POST `/batch`

Submit multiple **single** SQL statements to run either:
- in a **single transaction** (default), or
- in **autocommit** mode (for **read-only batches** when `AUTOCOMMIT_READONLY=true`).

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
- Limits (defaults): `MAX_STATEMENTS` = **50**, `MAX_SQL_LENGTH` = **100000**.

**Behavior**
- A transaction is started (`BEGIN`), statements are executed in order, then committed.  
  If the batch is **read-only** and `AUTOCOMMIT_READONLY=true`, each statement is executed inside its **own transaction** (cursor-safe for PgBouncer).  
- Failures:
  - In single-transaction mode → the transaction is rolled back, an error is streamed, and the batch is marked **aborted**.  
  - In autocommit mode → only the failing statement’s txn is rolled back; the batch streams an error and **aborted** event.

**Stream Payloads (logical shapes)**

NDJSON-style objects (exact same shapes are used as the `data` for SSE and as the documents for MsgPack/CBOR):

```jsonc
{"batch":{"begin":true,"statements":2,"requestId":"..."}}
{"statement":0,"row":{"n":1}}
{"statement":0,"summary":{"command":"SELECT","rowCount":null}}
{"statement":1,"summary":{"command":"SELECT","rowCount":null}}
{"batch":{"committed":true,"dbDurationMs":17}}
```

### Health

- `GET /healthz` → `{ "ok": true }`  
- `GET /ready` → process readiness; optionally includes DB probe results when `READY_CHECK_DB=1` and probe credentials provided.

---

## Examples

> Replace credentials, host, and SQL as needed.

### Examples: NDJSON

```bash
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "x-response-format: ndjson" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select 1 as n"}]}'
```

### Examples: SSE

```bash
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "accept: text/event-stream" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select generate_series(1,5) as n"}]}'
```

### Examples: MessagePack

```bash
# Save binary response to a file
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "x-response-format: msgpack" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select 1 as n"}]}' > out.msgpack
```

### Examples: CBOR

```bash
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "x-response-format: cbor" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select 1 as n"}]}' > out.cbor
```

### Examples: CSV

```bash
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "x-response-format: csv" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select 1 as n"}]}' | head -20
```

### Examples: Apache Arrow IPC

```bash
curl -sN \
  -H "authorization: Basic $(printf 'alice:secret' | base64)" \
  -H "content-type: application/json" \
  -H "x-response-format: arrow" \
  -X POST http://localhost:8000/batch \
  --data '{"statements":[{"sql":"select 1 as n"}]}' > out.arrow
```

> **Client decoding tips**
>
> - **MsgPack**: Use `@msgpack/msgpack` and a *multi-document* decoder to iterate concatenated documents.
> - **CBOR**: Use `cbor-x` and its multi-document decode helpers.
> - **Arrow**: Use `apache-arrow`’s `RecordBatchReader`(JS) or `pyarrow.flight` (Python) to iterate batches. Clients must accept **concatenated** IPC streams.

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
  `authorization, content-type, x-db-user, x-statement-timeout-ms, x-statement-timeout-per-index, x-lock-timeout-ms, x-lock-timeout-per-index, x-idle-in-txn-timeout-ms, x-fetch-size, x-flush-every, x-flush-interval-ms, x-response-format, x-app-name, x-allow-mutations, x-perf-debug`

Preflight responses return `204` with a max-age of 86400 seconds.

---

## Development

- `src/shared/env.ts` provides `envBool` used across handlers.  
- Connect-time errors are classified in `src/pipeline/execute/db/errors.ts` and respected both in retry logic and the `/batch` pre-connect path.  
- `StreamEncoder` is defined in `src/pipeline/stream/encoders/types.ts` and shared by all format implementations.  
- Modify defaults in `src/shared/constants.ts`.  
- DB adapter lives in `src/pipeline/execute/db/adapter.ts`.

Run locally:

```bash
deno run -A src/main.ts
# visit: http://localhost:8000/healthz
```

---

## Performance Tips

- **`x-fetch-size`**: larger values reduce round trips but increase memory per chunk.  
- **`x-flush-every`**: `0` for lowest latency; higher values batch encoder writes.  
- **`x-flush-interval-ms`**: pair with a nonzero buffer to bound latency under low row rates.  
- **Per-index timeouts**: e.g., make a long-running statement tolerant without penalizing others:  
  `x-statement-timeout-per-index: 0=200,2=5000`

---

## Benchmarks & Tuning

We ship a micro-benchmark to compare encoders and buffering policies.

**Script:** `bench/stream_formats.bench.ts`  
**Run:**

```bash
deno bench -A bench/stream_formats.bench.ts
```

**Environment knobs:**

| Var | Default | Meaning |
| --- | ---: | --- |
| `BENCH_STATEMENTS` | 3 | How many statements in the batch. |
| `BENCH_ROWS_PER_STATEMENT` | 4000 | Rows per statement. |
| `BENCH_FLUSH_EVERY` | `"0,64"` | Comma-separated list of `flushEvery` values (each becomes a bench group). |
| `BENCH_FLUSH_INTERVAL_MS` | 0 | Periodic flush interval. |
| `BENCH_WIDE_STR_BYTES` | 64 | Approx bytes for the wide text column. |

**Notes:** rows are pre‑serialized once to focus measurement on encoder/stream overhead. Streams are fully drained to include backpressure and enqueue costs.

---

## Notes

- **Transaction model**
  - Default: the executor wraps the batch in a single transaction.  
    - Success → `COMMIT` and `{"batch":{"committed":true,...}}`  
    - Failure → `ROLLBACK` and `{"batch":{"aborted":true}}` with an `error` event.
  - When `AUTOCOMMIT_READONLY=true` *and* the batch is **read-only**, each statement runs in its **own** short transaction (cursor-friendly for PgBouncer).

- **Statement classification**
  - SELECT-like detection controls streaming (`SELECT`, `VALUES`, `TABLE`, `SHOW`, `EXPLAIN`).  
  - PgBouncer-unsafe commands are rejected in strict mode (`SET`, `LISTEN/UNLISTEN`, `NOTIFY`, `COPY ... STDIN/STDOUT`, `BEGIN/COMMIT/ROLLBACK`, savepoints).

- **Perf instrumentation**
  - Toggle with header `x-perf-debug: 1` or env `PERF_DEBUG=1`.  
  - Logs JSON lines (`level=debug`, `event=perf`) with span durations and optional heap metrics.

