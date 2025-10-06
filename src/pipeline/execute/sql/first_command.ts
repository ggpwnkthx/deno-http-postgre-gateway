/**
 * Extract the first top-level SQL command keyword (e.g., select/insert/with/...),
 * skipping comments, strings, dollar-quotes, and CTE bodies (`WITH ...`).
 *
 * NOTE: exported for reuse by high-level helpers; not re-exported publicly unless needed.
 */
import { isWordChar, isWordCharOrDigit } from "./char.ts";
import { findClosingDollar, readDollarTag } from "./dollar.ts";

const COMMAND_WORDS = new Set([
  // DML
  "select",
  "insert",
  "update",
  "delete",
  "values",
  "merge",
  // Utility-ish / misc (for detection/guarding elsewhere)
  "table",
  "show",
  "explain",
  "call",
  "do",
  "create",
  "alter",
  "drop",
  "truncate",
  "grant",
  "revoke",
  "set",
  "listen",
  "unlisten",
  "notify",
  "prepare",
  "deallocate",
  "copy",
  "begin",
  "commit",
  "rollback",
  "savepoint",
  "release",
  "analyze",
  "vacuum",
]);

export function firstTopLevelCommand(sql: string): string | null {
  const s = sql.trim();
  const n = s.length;
  let i = 0;
  let inS = false, inD = false, inLine = false, inBlock = false;
  let dollarTag: string | null = null;
  let depth = 0;
  let skippingCte = false;

  function readWord(): string {
    const start = i;
    if (!isWordChar(s[i])) return "";
    i++;
    while (i < n && isWordCharOrDigit(s[i])) i++;
    return s.slice(start, i).toLowerCase();
  }

  while (i < n) {
    const c = s[i], nx = s[i + 1];

    // comment/string states
    if (inLine) {
      if (c === "\n") inLine = false;
      i++;
      continue;
    }
    if (inBlock) {
      if (c === "*" && nx === "/") {
        inBlock = false;
        i += 2;
      } else i++;
      continue;
    }
    if (dollarTag !== null) {
      const close = findClosingDollar(s, i, dollarTag);
      if (close === -1) return null;
      i = close;
      dollarTag = null;
      continue;
    }
    if (inS) {
      if (c === "'" && nx === "'") {
        i += 2;
        continue;
      }
      if (c === "'") {
        inS = false;
        i++;
        continue;
      }
      i++;
      continue;
    }
    if (inD) {
      if (c === '"') {
        inD = false;
        i++;
        continue;
      }
      i++;
      continue;
    }

    // whitespace
    if (c === " " || c === "\t" || c === "\r" || c === "\n") {
      i++;
      continue;
    }

    // comments
    if (c === "-" && nx === "-") {
      inLine = true;
      i += 2;
      continue;
    }
    if (c === "/" && nx === "*") {
      inBlock = true;
      i += 2;
      continue;
    }

    // strings (single/double/dollar)
    const dq = readDollarTag(s, i);
    if (dq) {
      dollarTag = dq.tag;
      i = dq.next;
      continue;
    }
    if (c === "'") {
      inS = true;
      i++;
      continue;
    }
    if (c === '"') {
      inD = true;
      i++;
      continue;
    }

    // parens control depth (to skip CTE bodies)
    if (c === "(") {
      depth++;
      i++;
      continue;
    }
    if (c === ")") {
      if (depth > 0) depth--;
      i++;
      continue;
    }

    // At top level, look for command words.
    if (depth === 0 && isWordChar(c)) {
      const word = readWord();

      if (word === "with") {
        skippingCte = true;
        continue;
      }

      if (skippingCte) {
        // Skip CTE names and 'recursive'; resume when we see a true command word.
        if (word === "recursive") continue;
        if (COMMAND_WORDS.has(word)) {
          skippingCte = false;
          return word;
        }
        // Not a command word → it's part of CTE list; keep scanning.
        continue;
      }

      // Normal path: return only recognized command words.
      if (COMMAND_WORDS.has(word)) return word;

      // Unknown token at top-level; continue scanning rather than returning garbage.
      continue;
    }

    i++;
  }

  return null;
}
