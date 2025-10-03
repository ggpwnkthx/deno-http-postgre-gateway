/** Character classification helpers used by the lightweight SQL scanners. */

export function isWordChar(ch: string) {
  const code = ch.charCodeAt(0);
  // A-Z a-z _
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || ch === "_";
}

export function isWordCharOrDigit(ch: string) {
  const code = ch.charCodeAt(0);
  // 0-9
  return isWordChar(ch) || (code >= 48 && code <= 57);
}
