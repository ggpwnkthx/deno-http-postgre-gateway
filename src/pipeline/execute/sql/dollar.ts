import { isWordCharOrDigit } from "./char.ts";

/** Try to read a dollar-quote tag at position i (e.g., $tag$ or $$). Returns the tag and next index after the opening delimiter. */
export function readDollarTag(s: string, i: number): { tag: string; next: number } | null {
  if (s[i] !== "$") return null;
  let j = i + 1;
  while (j < s.length && isWordCharOrDigit(s[j])) j++;
  if (s[j] === "$") {
    // $tag$ or $$ (tag may be empty)
    const tag = s.slice(i + 1, j);
    return { tag, next: j + 1 };
  }
  return null;
}

/** Find the closing $tag$ (or $$) starting at index i. Returns index just after the closing delimiter or -1 if not found. */
export function findClosingDollar(s: string, i: number, tag: string): number {
  const needle = `$${tag}$`;
  const k = s.indexOf(needle, i);
  return k === -1 ? -1 : k + needle.length;
}
