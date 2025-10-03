import { firstTopLevelCommand } from "./first_command.ts";

/**
 * Detect top-level commands that are unsafe/useless with PgBouncer transaction pooling
 * or conflict with this gateway's transaction management.
 */
export function detectPgBouncerUnsafe(sql: string): string[] {
  const cmd = firstTopLevelCommand(sql)?.toLowerCase();
  if (!cmd) return [];

  const disallowed = new Set([
    "set", // session settings; only internal SET LOCAL is allowed
    "listen",
    "unlisten",
    "notify",
    "prepare",
    "deallocate",
    "copy", // COPY ... STDIN/STDOUT not supported by this gateway
    "begin",
    "commit",
    "rollback",
    "savepoint",
    "release",
  ]);

  // Handle multi-word "prepare transaction" specifically for clearer error messaging.
  if (cmd === "prepare") {
    const s = sql.trim().toLowerCase();
    if (s.startsWith("prepare transaction")) return ["prepare transaction"];
  }

  return disallowed.has(cmd) ? [cmd] : [];
}
