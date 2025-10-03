// --- Environment (.env)
export { load as dotenvLoad } from "jsr:@std/dotenv@0.225.5";

// --- Encoding
export { decodeBase64 } from "jsr:@std/encoding@1/base64";

// --- PostgreSQL driver
export { Client, type ClientOptions } from "jsr:@db/postgres";

// --- Binary encodings
export { encode as msgpackEncode } from "npm:@msgpack/msgpack@^3";
export { encode as cborEncode } from "npm:cbor-x@^1";

// --- Apache Arrow (IPC)
export { RecordBatchWriter, tableFromJSON } from "npm:apache-arrow@15.0.2";
