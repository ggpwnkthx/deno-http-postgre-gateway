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
    async streamRows(sql, params, fetchSize, onRow) {
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

        let useObjectMode = true;
        let colKeyParts: string[] | null = null;

        if (Array.isArray(cols) && cols.length > 0) {
          useObjectMode = false;
          colKeyParts = buildColKeyParts(cols);
        }

        while (true) {
          if (useObjectMode) {
            // Object mode for drivers that don't expose rowDescription on array mode.
            const objRes = await client.queryObject<Record<string, unknown>>(
              `FETCH FORWARD ${fetchSize} FROM ${cursor}`,
            );
            if (objRes.rows.length === 0) break;
            for (const row of objRes.rows) {
              (onRow as unknown as (row: string) => void)(JSON.stringify(row));
            }
            if (objRes.rows.length < fetchSize) break;
            continue;
          }

          // Fast array mode with precomputed key parts.
          const res = await client.queryArray(`FETCH FORWARD ${fetchSize} FROM ${cursor}`);
          const rows = (res as unknown as { rows: unknown[][] }).rows;
          if (rows.length === 0) break;

          const parts = colKeyParts!;
          for (const row of rows) {
            const json = rowArrayToJsonFast(parts, row as unknown[]);
            (onRow as unknown as (row: string) => void)(json);
          }

          if (rows.length < fetchSize) break;
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
