import { abToBase64, createChunkStream, encodeUtf8, u8ToBase64 } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

/**
 * CSV encoder pushing bytes directly:
 * - Emits tokens/cells as separate chunks (no big string joins).
 * - Header cached per statement; comment/meta lines emitted as bytes.
 * - Prefers array/object rows (no JSON.parse on hot path).
 */
export function createCsvEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, pushBytes, close } = createChunkStream(
    flushEvery,
    flushIntervalMs,
    undefined,
    onCancel,
    perf,
  );

  // Per-statement schema and header cache
  const colsByStmt = new Map<number, string[]>();
  const headerEmitted = new Set<number>();

  const NL = encodeUtf8("\n");
  const COMMA = encodeUtf8(",");
  const HASH_SPACE = encodeUtf8("# ");
  const SECTION_PFX_CACHE = new Map<number, Uint8Array>();

  const sectionHeaderBytes = (i: number): Uint8Array => {
    let u8 = SECTION_PFX_CACHE.get(i);
    if (!u8) {
      u8 = encodeUtf8(`# statement:${i}\n`);
      SECTION_PFX_CACHE.set(i, u8);
    }
    return u8;
  };

  const emitComment = (obj: unknown) => {
    try {
      pushBytes(HASH_SPACE);
      pushBytes(encodeUtf8(JSON.stringify(obj)));
      pushBytes(NL);
    } catch {
      // best effort
    }
  };

  const csvEscape = (s: string): string => {
    if (
      s.indexOf('"') === -1 && s.indexOf(",") === -1 && s.indexOf("\n") === -1 &&
      s.indexOf("\r") === -1
    ) {
      return s;
    }
    return `"${s.replace(/"/g, '""')}"`;
  };

  const toCsvCell = (v: unknown): string => {
    if (v instanceof Uint8Array) v = u8ToBase64(v);
    else if (v instanceof ArrayBuffer) v = abToBase64(v);

    if (v === null || v === undefined) return "";
    switch (typeof v) {
      case "string":
        return csvEscape(v);
      case "number":
      case "bigint":
      case "boolean":
        return String(v);
      default:
        try {
          return csvEscape(JSON.stringify(v));
        } catch {
          return csvEscape(String(v));
        }
    }
  };

  const emitHeaderIfNeeded = (i: number, cols: string[]) => {
    if (headerEmitted.has(i)) return;
    pushBytes(sectionHeaderBytes(i));
    for (let c = 0; c < cols.length; c++) {
      if (c > 0) pushBytes(COMMA);
      pushBytes(encodeUtf8(csvEscape(cols[c])));
    }
    pushBytes(NL);
    headerEmitted.add(i);
  };

  return {
    stream,

    onSchema: (i, columns) => {
      colsByStmt.set(i, columns.slice());
    },

    emitArrayRow: (i, row) => {
      let cols = colsByStmt.get(i);
      if (!cols) {
        cols = row.map((_v, idx) => `col${idx + 1}`);
        colsByStmt.set(i, cols);
      }
      emitHeaderIfNeeded(i, cols);

      for (let c = 0; c < row.length; c++) {
        if (c > 0) pushBytes(COMMA);
        pushBytes(encodeUtf8(toCsvCell(row[c])));
      }
      pushBytes(NL);
    },

    emit: (type, payload) => {
      if (type === "row" && payload && typeof payload === "object") {
        const { statement, row } = payload as { statement: number; row: Record<string, unknown> };
        if (!row || typeof row !== "object" || Array.isArray(row)) {
          emitComment({ statement, row });
          return;
        }

        let cols = colsByStmt.get(statement);
        if (!cols) {
          cols = Object.keys(row);
          colsByStmt.set(statement, cols);
        }
        emitHeaderIfNeeded(statement, cols);

        for (let c = 0; c < cols.length; c++) {
          if (c > 0) pushBytes(COMMA);
          pushBytes(encodeUtf8(toCsvCell(row[cols[c]])));
        }
        pushBytes(NL);
        return;
      }

      // Non-row events -> comment lines (JSON)
      emitComment(payload);
    },

    preferArrays: true,
    preferObjects: true,

    close,
  };
}
