import type { DbClient, QuerySummary } from "../../../shared/types.ts";
import type { Client as PgClient } from "../../../deps.ts";
import { seemsSelectLike } from "../sql/seems_select_like.ts";

/** Per-connection cursor sequence to avoid random UUID overhead. */
let CURSOR_SEQ = 0;
function nextCursorName() {
  CURSOR_SEQ = (CURSOR_SEQ + 1) >>> 0;
  return `c_${CURSOR_SEQ.toString(36)}`;
}

/** Base64 encode Uint8Array without huge apply() frames. */
function toBase64(u8: Uint8Array): string {
  let bin = "";
  // NOTE: Plain loop is consistently fast and GC-light in V8.
  for (let i = 0; i < u8.length; i++) bin += String.fromCharCode(u8[i]);
  // btoa expects Latin-1 strings; this is ok for raw bytes.
  return btoa(bin);
}

/** Serialize a JS value the same way jsonReplacer would shape it. */
function jsonScalar(v: unknown): string {
  if (typeof v === "bigint") return JSON.stringify(v.toString());
  if (v instanceof Date) return JSON.stringify(v.toISOString());
  if (v instanceof Uint8Array) return JSON.stringify(`base64:${toBase64(v)}`);
  if (v instanceof ArrayBuffer) return JSON.stringify(`base64:${toBase64(new Uint8Array(v))}`);
  return JSON.stringify(v);
}

/** Precompute `"col":` fragments once per statement. */
function buildColKeyParts(colNames: string[]): string[] {
  const parts = new Array(colNames.length);
  for (let i = 0; i < colNames.length; i++) {
    parts[i] = JSON.stringify(colNames[i]) + ":";
  }
  return parts;
}

/** Serialize an array of column values to a JSON object using prebuilt key parts. */
function rowArrayToJsonFast(colKeyParts: string[], row: unknown[]): string {
  let s = "{";
  for (let i = 0; i < colKeyParts.length; i++) {
    if (i > 0) s += ",";
    s += colKeyParts[i];
    s += jsonScalar(row[i]);
  }
  s += "}";
  return s;
}

export function toDbClientAPI(client: PgClient): DbClient {
  return {
    async begin() {
      await client.queryArray("BEGIN");
    },
    async commit() {
      await client.queryArray("COMMIT");
    },
    async rollback() {
      await client.queryArray("ROLLBACK");
    },
    async execute(sql: string, params?: unknown[]): Promise<QuerySummary> {
      // Use queryArray to avoid object materialization when not needed.
      const res = await client.queryArray({ text: sql, args: params ?? [] });
      const command = (res as unknown as { command?: string }).command ??
        (seemsSelectLike(sql) ? "SELECT" : "QUERY");
      const rowCount = (res as unknown as { rowCount?: number }).rowCount ?? null;
      return { command, rowCount };
    },

    async streamRows(sql, params, fetchSize, onRow, opts): Promise<QuerySummary> {
      const mode = opts?.as ?? "json";
      const cursor = nextCursorName();

      await client.queryArray({
        text: `DECLARE ${cursor} NO SCROLL CURSOR FOR ${sql}`,
        args: params ?? [],
      });

      try {
        // Peek metadata without consuming rows.
        const peek = await client.queryArray(`FETCH FORWARD 0 FROM ${cursor}`);
        const anyPeek = peek as unknown as {
          rowDescription?: { columns?: Array<{ name: string }> };
        };
        const cols = anyPeek.rowDescription?.columns?.map((c) => c.name);

        const haveCols = Array.isArray(cols) && cols.length > 0;
        if (haveCols && typeof opts?.onSchema === "function") {
          // Inform downstream encoder/executor once per statement.
          opts.onSchema(cols!);
        }

        const colKeyParts = haveCols ? buildColKeyParts(cols!) : null;

        while (true) {
          if (mode === "array") {
            // Pure array mode: no per-row object creation here.
            const res = await client.queryArray(`FETCH FORWARD ${fetchSize} FROM ${cursor}`);
            const rows = (res as unknown as { rows: unknown[][] }).rows;
            if (rows.length === 0) break;
            for (const r of rows) onRow(r as unknown[]);
            if (rows.length < fetchSize) break;
            continue;
          }

          if (mode === "object") {
            if (haveCols) {
              // Array mode + manual mapping to POJO → no JSON stringify/parse.
              const res = await client.queryArray(`FETCH FORWARD ${fetchSize} FROM ${cursor}`);
              const rows = (res as unknown as { rows: unknown[][] }).rows;
              if (rows.length === 0) break;

              const names = cols!;
              for (const row of rows) {
                const r = row as unknown[];
                const obj: Record<string, unknown> = {};
                for (let i = 0; i < names.length; i++) obj[names[i]] = r[i];
                onRow(obj);
              }
              if (rows.length < fetchSize) break;
              continue;
            } else {
              // Fallback: driver object mode when rowDescription isn't available.
              const objRes = await client.queryObject<Record<string, unknown>>(
                `FETCH FORWARD ${fetchSize} FROM ${cursor}`,
              );
              if (objRes.rows.length === 0) break;
              for (const row of objRes.rows) onRow(row);
              if (objRes.rows.length < fetchSize) break;
              continue;
            }
          }

          // mode === "json" (pre-serialized JSON for text encoders)
          if (haveCols) {
            const res = await client.queryArray(`FETCH FORWARD ${fetchSize} FROM ${cursor}`);
            const rows = (res as unknown as { rows: unknown[][] }).rows;
            if (rows.length === 0) break;
            const parts = colKeyParts!;
            for (const row of rows) {
              onRow(rowArrayToJsonFast(parts, row as unknown[]));
            }
            if (rows.length < fetchSize) break;
            continue;
          } else {
            const objRes = await client.queryObject<Record<string, unknown>>(
              `FETCH FORWARD ${fetchSize} FROM ${cursor}`,
            );
            if (objRes.rows.length === 0) break;
            for (const row of objRes.rows) onRow(JSON.stringify(row));
            if (objRes.rows.length < fetchSize) break;
            continue;
          }
        }

        return { command: "SELECT", rowCount: null };
      } finally {
        try {
          await client.queryArray(`CLOSE ${cursor}`);
        } catch {
          /* ignore */
        }
      }
    },

    async end() {
      await client.end();
    },
  };
}
