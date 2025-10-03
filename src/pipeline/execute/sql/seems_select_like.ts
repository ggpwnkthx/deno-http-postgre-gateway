import { firstTopLevelCommand } from "./first_command.ts";

/** Heuristics: commands that yield row sets in a cursor-friendly way. */
export function seemsSelectLike(sql: string): boolean {
  const cmd = firstTopLevelCommand(sql);
  if (!cmd) return false;
  return (
    cmd === "select" ||
    cmd === "values" ||
    cmd === "table" ||
    cmd === "show" ||
    cmd === "explain"
  );
}
