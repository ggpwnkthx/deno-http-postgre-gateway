import { json } from "../../../shared/http.ts";
import { getClientFactory, readDbEnv } from "../../plan/build.ts";
import { envBool } from "../../../shared/env.ts";

/**
 * GET /ready
 *
 * - Always returns basic process readiness.
 * - If READY_CHECK_DB=1, attempts a fast connect + "select 1" using
 *   PGREADY_USER/PGREADY_PASSWORD when provided, otherwise rejects the DB check.
 *   (Avoids accidentally using end-user creds in a probe.)
 *
 * Refactor: run the probe query without wrapping it in an explicit transaction
 * (autocommit is sufficient and cheaper for this health check).
 */
export async function handleReady(_req: Request): Promise<Response> {
  const probeDb = envBool("READY_CHECK_DB", false);

  const base = {
    ok: true,
    pid: Deno.pid,
    deno: Deno.version,
  };

  if (!probeDb) {
    return json(base);
  }

  const readyUser = Deno.env.get("PGREADY_USER");
  const readyPass = Deno.env.get("PGREADY_PASSWORD");

  if (!readyUser || !readyPass) {
    return json({
      ...base,
      db: { checked: false, reason: "missing_probe_credentials" },
    });
  }

  try {
    const env = readDbEnv();
    const getClient = getClientFactory(env, "ready-probe", {
      user: readyUser,
      password: readyPass,
    });
    const client = await getClient();
    try {
      // Autocommit is sufficient for a liveness probe.
      await client.execute("SELECT 1");
    } finally {
      await client.end();
    }
    return json({ ...base, db: { checked: true, ok: true } });
  } catch (e) {
    // Surface probe failure but keep HTTP 200 so external orchestrators don’t flappy-kill on DB hiccups unless you wire it differently.
    return json({
      ...base,
      db: { checked: true, ok: false, message: e instanceof Error ? e.message : String(e) },
    });
  }
}
