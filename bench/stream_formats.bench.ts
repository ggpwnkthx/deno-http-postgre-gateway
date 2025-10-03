// Benchmarks for the various stream encoders in src/pipeline/stream/encoders/*
// Usage:
//   deno bench -A bench/stream_formats.bench.ts
//
// Env knobs (optional):
//   BENCH_STATEMENTS            default: 3
//   BENCH_ROWS_PER_STATEMENT    default: 4000
//   BENCH_FLUSH_EVERY           default: "0,64"   (comma-separated list; each becomes a bench group)
//   BENCH_FLUSH_INTERVAL_MS     default: 0        (applies to all benches)
//   BENCH_WIDE_STR_BYTES        default: 64       (size of a wide string field per row)
//
// Notes:
// - We precompute row JSON strings outside the measured function to focus the
//   benchmark on encoder + stream overhead, not row generation/JSON.stringify.
// - We use the encoders' `emitRow` fast path everywhere (including Arrow,
//   which accumulates rows until it sees a "summary" for that statement).
// - We fully drain the produced stream in each run so backpressure and
//   enqueue costs are included.

import { createNdjsonEncoder } from "../src/pipeline/stream/encoders/ndjson.ts";
import { createSseEncoder } from "../src/pipeline/stream/encoders/sse.ts";
import { createMsgpackEncoder } from "../src/pipeline/stream/encoders/msgpack.ts";
import { createCborEncoder } from "../src/pipeline/stream/encoders/cbor.ts";
import { createCsvEncoder } from "../src/pipeline/stream/encoders/csv.ts";
import { createArrowIpcEncoder } from "../src/pipeline/stream/encoders/arrow.ts";
import type { StreamEncoder } from "../src/pipeline/stream/encoders/types.ts";
import { jsonReplacer } from "../src/pipeline/stream/encoders/base.ts";

// ------------------------- config -------------------------

function intEnv(name: string, def: number): number {
  const v = Deno.env.get(name);
  if (!v) return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function listEnv(name: string, def: number[]): number[] {
  const v = Deno.env.get(name);
  if (!v) return def;
  const parts = v.split(",").map((s) => s.trim()).filter(Boolean);
  const nums = parts.map((p) => Number(p)).filter((n) => Number.isFinite(n) && n >= 0);
  return nums.length ? nums : def;
}

const STATEMENTS = intEnv("BENCH_STATEMENTS", 3);
const ROWS_PER_STMT = intEnv("BENCH_ROWS_PER_STATEMENT", 4000);
const FLUSH_EVERY_LIST = listEnv("BENCH_FLUSH_EVERY", [0, 64]);
const FLUSH_INTERVAL_MS = intEnv("BENCH_FLUSH_INTERVAL_MS", 0);
const WIDE_STR_BYTES = intEnv("BENCH_WIDE_STR_BYTES", 64);

// --------------------- synthetic dataset ------------------

/** deterministic small binary for each row index */
function makeBytes(i: number, len: number): Uint8Array {
  const u8 = new Uint8Array(len);
  for (let j = 0; j < len; j++) u8[j] = (i * 131 + j * 17) & 0xff;
  return u8;
}

/** deterministic human string payload of ~WIDE_STR_BYTES */
function makeWideString(i: number, bytes: number): string {
  // produce a repeated base36-ish chunk to reach approx N bytes
  const chunk = (i.toString(36) + "_").padEnd(12, "x");
  const reps = Math.max(1, Math.ceil(bytes / chunk.length));
  return (chunk.repeat(reps)).slice(0, bytes);
}

/** Pre-generate one statement's worth of JSON row strings using the same scalar shaping our pipeline uses. */
function buildRowJsonCorpus(count: number): string[] {
  const arr = new Array<string>(count);
  for (let i = 0; i < count; i++) {
    const row = {
      id: i,
      name: `row_${i}`,
      // exercise mixed scalars / complex values
      ts: new Date(1_700_000_000_000 + i * 1000),          // Date -> ISO string via jsonReplacer
      big: BigInt(9007199254740991n + BigInt(i)),          // BigInt -> string via jsonReplacer
      flag: (i & 1) === 0,
      data: makeBytes(i, 24),                               // Uint8Array -> base64:... via jsonReplacer
      arr: [i, i + 1, i + 2],
      obj: { a: i, b: `b${i}` },
      wide: makeWideString(i, WIDE_STR_BYTES),              // wider text column
    };
    arr[i] = JSON.stringify(row, jsonReplacer);
  }
  return arr;
}

// One corpus reused across all statements/benches to avoid counting gen cost
const ROW_JSONS = buildRowJsonCorpus(ROWS_PER_STMT);

// -------------------------- runner ------------------------

type Factory = (flushEvery: number, flushIntervalMs: number) => StreamEncoder;

const factories: Record<string, Factory> = {
  sse: (fe, fi) => createSseEncoder(fe, fi),
  ndjson: (fe, fi) => createNdjsonEncoder(fe, fi),
  csv: (fe, fi) => createCsvEncoder(fe, fi),
  msgpack: (fe, fi) => createMsgpackEncoder(fe, fi),
  cbor: (fe, fi) => createCborEncoder(fe, fi),
  arrow: (fe, fi) => createArrowIpcEncoder(fe, fi),
};

/** Drain a ReadableStream<Uint8Array> completely (count bytes to keep the engine honest). */
async function drain(stream: ReadableStream<Uint8Array>): Promise<number> {
  const r = stream.getReader();
  let total = 0;
  while (true) {
    const { done, value } = await r.read();
    if (done) break;
    if (value) total += value.byteLength;
  }
  return total;
}

/** Drive a complete "batch" for a single encoder instance. */
async function driveEncoder(enc: StreamEncoder) {
  // Start draining immediately so enqueue costs are realistic.
  const drainP = drain(enc.stream);

  // Begin event (shape mirrors our pipeline's evt.begin)
  enc.emit("batch", { batch: { begin: true, statements: STATEMENTS, requestId: "bench" } });

  // Produce rows for each statement, then a summary (triggers Arrow flush)
  for (let s = 0; s < STATEMENTS; s++) {
    for (let i = 0; i < ROW_JSONS.length; i++) {
      const rowJson = ROW_JSONS[i];
      if (enc.emitRow) {
        enc.emitRow(s, rowJson);
      } else {
        // Fallback (should not hit; all encoders here implement emitRow)
        try {
          enc.emit("row", { statement: s, row: JSON.parse(rowJson) });
        } catch {
          enc.emit("row", { statement: s, row: rowJson });
        }
      }
    }
    enc.emit("summary", { statement: s, summary: { command: "SELECT", rowCount: ROW_JSONS.length } });
  }

  // Commit + close
  enc.emit("batch", { batch: { committed: true, dbDurationMs: 0 } });
  enc.close();

  // Ensure everything is consumed
  const bytes = await drainP;
  // Tiny assertion to ensure we actually produced bytes.
  if (bytes <= 0) throw new Error("encoder produced no output");
}

// -------------------------- benches -----------------------

// Group by flush policy; use NDJSON as the baseline within each group for easy comparisons.
for (const [name, factory] of Object.entries(factories)) {
  for (const flushEvery of FLUSH_EVERY_LIST) {
    const group = `flushEvery=${flushEvery}, intervalMs=${FLUSH_INTERVAL_MS}`;
    const title = `${name} | ${group}`;
    Deno.bench({
      name: title,
      group,
      baseline: name === "ndjson",
      async fn() {
        const enc = factory(flushEvery, FLUSH_INTERVAL_MS);
        await driveEncoder(enc);
      },
    });
  }
}
