// --- Environment (.env)
export { load as dotenvLoad } from "jsr:@std/dotenv@0.225.5";

// --- Encoding
export { decodeBase64 } from "jsr:@std/encoding@1/base64";

// --- PostgreSQL driver
export { Client, type ClientOptions } from "jsr:@db/postgres";

// --- Binary encodings
export {
  decode as msgpackDecode,
  encode as msgpackEncode,
  type ValueMap as msgpackValueMap,
  type ValueType as msgpackValueType,
} from "jsr:@std/msgpack@^1";
export { encode as cborEncode } from "npm:cbor-x@^1";

// --- Apache Arrow (IPC)
export { RecordBatchReader, RecordBatchWriter, tableFromJSON } from "npm:apache-arrow@^21";
