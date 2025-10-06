import { assert, assertMatch } from "jsr:@std/assert@^1";
import { createStreamEncoder } from "../src/pipeline/stream/encoder.ts";
import { streamToText, streamToU8 } from "./helpers/streams.ts";
import { jsonReplacer } from "../src/pipeline/stream/encoders/base.ts";
import { decodeMultiple as cborDecodeMultiple } from "npm:cbor-x@^1";
import { msgpackDecode, msgpackEncode, RecordBatchReader } from "../src/deps.ts";

const ROW = { id: 1, name: "row_1", data: new Uint8Array([1, 2, 3]) };

type RowsDoc = { rows: Array<{ statement: number; row: Record<string, unknown> }> };

function isRowsDoc(v: unknown): v is RowsDoc {
  if (typeof v !== "object" || v === null) return false;
  const rows = (v as { rows?: unknown }).rows;
  return Array.isArray(rows);
}

function emitAll(enc: ReturnType<typeof createStreamEncoder>) {
  enc.emit("batch", { batch: { begin: true, statements: 1, requestId: "t" } });
  if (enc.emitRow) {
    const rowJson = JSON.stringify(ROW, jsonReplacer);
    enc.emitRow(0, rowJson);
  } else {
    enc.emit("row", { statement: 0, row: ROW });
  }
  enc.emit("summary", { statement: 0, summary: { command: "SELECT", rowCount: 1 } });
  enc.emit("batch", { batch: { committed: true, dbDurationMs: 1 } });
  enc.close();
}

Deno.test("NDJSON encoder: shapes lines as expected", async () => {
  const enc = createStreamEncoder("ndjson", 0, 0);
  emitAll(enc);
  const s = await streamToText(enc.stream);
  assertMatch(s, /"batch":\{"begin":true/);
  assertMatch(s, /"statement":0,"row":\{"id":1/);
  assertMatch(s, /"summary":\{"command":"SELECT"/);
});

Deno.test("SSE encoder: has :ok preface and event frames", async () => {
  const enc = createStreamEncoder("sse", 0, 0, undefined);
  emitAll(enc);
  const s = await streamToText(enc.stream);
  assertMatch(s, /^:ok\n\n/);
  assertMatch(s, /event: row\ndata: \{"statement":0,"row":/);
  assertMatch(s, /event: summary\ndata:/);
});

Deno.test("CSV encoder: section header + header row + data row", async () => {
  const enc = createStreamEncoder("csv", 0, 0);
  emitAll(enc);
  const t = await streamToText(enc.stream);
  assertMatch(t, /^# statement:0/m);
  assertMatch(t, /^id,name,data/m); // header row
  assertMatch(t, /base64:/); // binary sentinel
});

Deno.test("MsgPack encoder: batches rows + typed bytes", async () => {
  const enc = createStreamEncoder("msgpack", 0, 0);
  emitAll(enc);
  const u8 = await streamToU8(enc.stream);

  // Walk concatenated documents by re-encoding each decoded doc to determine its byte length.
  let pos = 0;
  let sawRows = false;
  while (pos < u8.length) {
    const dec = msgpackDecode(u8.subarray(pos));
    const re = msgpackEncode(dec);
    pos += re.length;

    if (isRowsDoc(dec)) {
      const firstRow = dec.rows[0]?.row as Record<string, unknown>;
      const dataVal = firstRow["data"];
      assert(dataVal instanceof Uint8Array);
      sawRows = true;
    }
  }
  assert(sawRows);
});

Deno.test("CBOR encoder: batches rows + typed bytes", async () => {
  const enc = createStreamEncoder("cbor", 0, 0);
  emitAll(enc);
  const u8 = await streamToU8(enc.stream);

  // decodeMultiple returns [] | void depending on whether a forEach is supplied.
  const maybeDocs = cborDecodeMultiple(u8) as unknown[] | void;
  const docs: unknown[] = Array.isArray(maybeDocs) ? maybeDocs : [];

  const rowsDoc = docs.find(isRowsDoc);
  if (!rowsDoc || !isRowsDoc(rowsDoc)) {
    throw new Error("rows document not found in CBOR stream");
  }
  const firstRow = rowsDoc.rows[0].row as Record<string, unknown>;
  const dataVal = firstRow["data"];
  assert(dataVal instanceof Uint8Array);
});

Deno.test("Arrow IPC encoder: produces non-empty concatenated IPC stream", async () => {
  const enc = createStreamEncoder("arrow", 1, 0); // minimal batch size -> more writers
  emitAll(enc);
  const u8 = await streamToU8(enc.stream);
  assert(u8.byteLength > 0);

  const reader = RecordBatchReader.from(u8);
  let totalRows = 0;
  for await (const batch of reader) {
    totalRows += batch.numRows;
  }
  assert(totalRows >= 1);
});

Deno.test("cancel triggers onCancel", async () => {
  let cancelled = false;
  const enc = createStreamEncoder("ndjson", 0, 0, () => {
    cancelled = true;
  });
  const reader = enc.stream.getReader();
  await reader.cancel();
  await Promise.resolve(); // allow cancel microtask to settle
  assert(cancelled);
});
