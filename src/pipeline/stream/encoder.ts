import type { ResponseFormat } from "../../shared/constants.ts";
import type { StreamEncoder } from "./encoders/types.ts";
import { createNdjsonEncoder } from "./encoders/ndjson.ts";
import { createSseEncoder } from "./encoders/sse.ts";
import { createMsgpackEncoder } from "./encoders/msgpack.ts";
import { createCborEncoder } from "./encoders/cbor.ts";
import { createCsvEncoder } from "./encoders/csv.ts";
import { createArrowIpcEncoder } from "./encoders/arrow.ts";
import { arrowIpcStream, cbor, csv, msgpack, ndjson, sse } from "../../shared/http.ts";
import type { PerfCtx } from "../../shared/perf.ts";

/** Factory to create the correct encoder for the requested format. */
export function createStreamEncoder(
  format: ResponseFormat,
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  switch (format) {
    case "sse":
      return createSseEncoder(flushEvery, flushIntervalMs, onCancel, perf);
    case "msgpack":
      return createMsgpackEncoder(flushEvery, flushIntervalMs, onCancel, perf);
    case "cbor":
      return createCborEncoder(flushEvery, flushIntervalMs, onCancel, perf);
    case "csv":
      return createCsvEncoder(flushEvery, flushIntervalMs, onCancel, perf);
    case "arrow":
      return createArrowIpcEncoder(flushEvery, flushIntervalMs, onCancel, perf);
    case "ndjson":
    default:
      return createNdjsonEncoder(flushEvery, flushIntervalMs, onCancel, perf);
  }
}

/** Factory to build the correct HTTP response for the stream. */
export function respondWithStream(
  format: ResponseFormat,
  stream: ReadableStream<Uint8Array>,
  init?: ResponseInit,
): Response {
  switch (format) {
    case "sse":
      return sse(stream, init);
    case "msgpack":
      return msgpack(stream, init);
    case "cbor":
      return cbor(stream, init);
    case "csv":
      return csv(stream, init);
    case "arrow":
      return arrowIpcStream(stream, init);
    case "ndjson":
    default:
      return ndjson(stream, init);
  }
}
