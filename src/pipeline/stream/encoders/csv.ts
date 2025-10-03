import { createLineStream } from "./base.ts";
import type { StreamEncoder } from "./types.ts";
import type { PerfCtx } from "../../../shared/perf.ts";

/**
 * CSV streaming:
 * - Writes metadata/events as comment lines starting with '# ' (so tools can ignore).
 * - For each statement index, emits a header row on first row ("col1,col2,...").
 * - Rows follow in stable column order derived from the first row.
 * - Non-scalar values are JSON-stringified in cells.
 *
 * Performance refactor:
 * - Cache the per-statement section header string.
 * - Avoid repeated string concatenations by prebuilding header+prefix once.
 */
export function createCsvEncoder(
  flushEvery: number,
  flushIntervalMs: number,
  onCancel?: () => void,
  perf?: PerfCtx,
): StreamEncoder {
  const { stream, push, close } = createLineStream(
    flushEvery,
    flushIntervalMs,
    undefined,
    onCancel,
    perf,
  );

  // Track per-statement column order and cached header strings
  const colsByStmt = new Map<number, string[]>();
  const sectionHeaderCache = new Map<number, string>();

  const sectionHeader = (i: number) => {
    let s = sectionHeaderCache.get(i);
    if (!s) {
      s = `# statement:${i}\n`;
      sectionHeaderCache.set(i, s);
    }
    return s;
  };

  function emitComment(obj: unknown) {
    try {
      push(`# ${JSON.stringify(obj)}\n`);
    } catch {
      // best effort
    }
  }

  function toCsvCell(v: unknown): string {
    // normalize special scalars used by jsonReplacer in other encoders
    if (v instanceof Uint8Array) {
      let bin = "";
      for (let i = 0; i < v.length; i++) bin += String.fromCharCode(v[i]);
      v = `base64:${btoa(bin)}`;
    } else if (v instanceof ArrayBuffer) {
      const view = new Uint8Array(v);
      let bin = "";
      for (let i = 0; i < view.length; i++) bin += String.fromCharCode(view[i]);
      v = `base64:${btoa(bin)}`;
    }

    if (v === null || v === undefined) return "";
    if (typeof v === "string") return csvEscape(v);
    if (typeof v === "number" || typeof v === "bigint" || typeof v === "boolean") {
      return String(v);
    }
    try {
      return csvEscape(JSON.stringify(v));
    } catch {
      return csvEscape(String(v));
    }
  }

  function csvEscape(s: string): string {
    const needs = /[",\n\r]/.test(s);
    const doubled = s.replace(/"/g, '""');
    return needs ? `"${doubled}"` : doubled;
  }

  return {
    stream,
    emit: (_type, payload) => {
      emitComment(payload);
    },
    emitRow: (i, rowJson) => {
      let obj: Record<string, unknown> | null = null;
      try {
        obj = JSON.parse(rowJson) as Record<string, unknown>;
      } catch {
        obj = null;
      }
      if (!obj || typeof obj !== "object") {
        emitComment({ statement: i, row: rowJson });
        return;
      }

      let cols = colsByStmt.get(i);
      if (!cols) {
        cols = Object.keys(obj);
        colsByStmt.set(i, cols);

        // Section header + header row (cache small prefix)
        push(sectionHeader(i));
        push(cols.map((c) => csvEscape(c)).join(",") + "\n");
      }

      const cells = cols.map((c) => toCsvCell((obj as Record<string, unknown>)[c]));
      push(cells.join(",") + "\n");
    },
    close,
  };
}
