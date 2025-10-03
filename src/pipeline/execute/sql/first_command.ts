/**
 * Extract the first top-level SQL command keyword (e.g., select/insert/with/...),
 * skipping comments, strings, dollar-quotes, and CTE bodies (`WITH ...`).
 *
 * NOTE: exported for reuse by high-level helpers; not re-exported publicly unless needed.
 */
import { isWordChar, isWordCharOrDigit } from "./char.ts";
import { findClosingDollar, readDollarTag } from "./dollar.ts";

export function firstTopLevelCommand(sql: string): string | null {
  const s = sql.trim();
  const n = s.length;
  let i = 0;
  let inS = false, inD = false, inLine = false, inBlock = false;
  let dollarTag: string | null = null;
  let depth = 0;

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
        // Skip entire CTE list; keep scanning for the real leading command.
        continue;
      }
      return word;
    }

    i++;
  }

  return null;
}
