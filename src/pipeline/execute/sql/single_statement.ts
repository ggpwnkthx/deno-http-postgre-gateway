/**
 * Single-statement detection.
 * Handles --, /* * /, $$...$$, $tag$...$tag$, '', "" without parsing SQL fully.
 */

import { findClosingDollar, readDollarTag } from "./dollar.ts";

export function isSingleStatement(sql: string): boolean {
  const s = sql;
  let i = 0;
  const n = s.length;
  let inS = false; // single quotes
  let inD = false; // double quotes (identifiers)
  let inLine = false;
  let inBlock = false;

  while (i < n) {
    const c = s[i];
    const nxt = s[i + 1];

    // inside line comment
    if (inLine) {
      if (c === "\n") inLine = false;
      i++;
      continue;
    }
    // inside block comment
    if (inBlock) {
      if (c === "*" && nxt === "/") {
        inBlock = false;
        i += 2;
      } else i++;
      continue;
    }
    // inside single-quoted string (with '' escapes)
    if (inS) {
      if (c === "'" && nxt === "'") {
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
    // inside double-quoted identifier
    if (inD) {
      if (c === '"') {
        inD = false;
        i++;
        continue;
      }
      i++;
      continue;
    }

    // not in any string/comment
    // detect start of comment
    if (c === "-" && nxt === "-") {
      inLine = true;
      i += 2;
      continue;
    }
    if (c === "/" && nxt === "*") {
      inBlock = true;
      i += 2;
      continue;
    }

    // detect start of dollar-quoted string
    const dq = readDollarTag(s, i);
    if (dq) {
      const close = findClosingDollar(s, dq.next, dq.tag);
      if (close === -1) {
        // unterminated; treat rest as string
        return true; // conservatively assume single statement
      }
      i = close;
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

    if (c === ";") {
      // ensure no non-whitespace/comment content afterwards
      let j = i + 1;
      let postLine = false, postBlock = false, postInS = false, postInD = false;
      while (j < n) {
        const ch = s[j], nx = s[j + 1];

        if (postLine) {
          if (ch === "\n") postLine = false;
          j++;
          continue;
        }
        if (postBlock) {
          if (ch === "*" && nx === "/") {
            postBlock = false;
            j += 2;
            continue;
          }
          j++;
          continue;
        }
        if (postInS) {
          if (ch === "'" && nx === "'") {
            j += 2;
            continue;
          }
          if (ch === "'") {
            postInS = false;
            j++;
            continue;
          }
          j++;
          continue;
        }
        if (postInD) {
          if (ch === '"') {
            postInD = false;
            j++;
            continue;
          }
          j++;
          continue;
        }

        // whitespace
        if (ch === " " || ch === "\t" || ch === "\r" || ch === "\n") {
          j++;
          continue;
        }

        // comments
        if (ch === "-" && nx === "-") {
          postLine = true;
          j += 2;
          continue;
        }
        if (ch === "/" && nx === "*") {
          postBlock = true;
          j += 2;
          continue;
        }

        // strings
        if (ch === "'") {
          postInS = true;
          j++;
          continue;
        }
        if (ch === '"') {
          postInD = true;
          j++;
          continue;
        }

        // dollar-quoted (skip)
        const postDQ = readDollarTag(s, j);
        if (postDQ) {
          const postClose = findClosingDollar(s, postDQ.next, postDQ.tag);
          if (postClose === -1) return true;
          j = postClose;
          continue;
        }

        // any other visible char after a top-level ';' → multi-statement
        return false;
      }
      // only whitespace/comments after ';'
      return true;
    }

    i++;
  }

  // no top-level ';' → single statement
  return true;
}
