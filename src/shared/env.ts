/** Parse boolean-like environment variables with sensible defaults. */
export function envBool(name: string, def = false): boolean {
  const v = (Deno.env.get(name) ?? "").trim().toLowerCase();
  if (!v) return def;
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return def;
}
