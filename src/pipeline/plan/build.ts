import type { DbClient } from "../../shared/types.ts";
import { connectPg } from "../execute/db/adapter.ts";

// Internal types
type StaticPassword = { user: string; password: string };
type ProviderPassword = { user: string; passwordProvider: () => Promise<string> | string };
type RoleCreds = StaticPassword | ProviderPassword;

function hasPassword(role: RoleCreds): role is StaticPassword {
  return "password" in role && typeof role.password === "string";
}

function hasPasswordProvider(role: RoleCreds): role is ProviderPassword {
  return "passwordProvider" in role && typeof role.passwordProvider !== "undefined";
}

/** Reads and validates required DB environment variables. */
export function readDbEnv() {
  const requireEnv = (name: string, optional = false): string | undefined => {
    const v = Deno.env.get(name);
    if (!v && !optional) throw new Error(`${name} is required`);
    return v;
  };

  const asInt = (name: string, def?: number) => {
    const v = Deno.env.get(name);
    if (!v) return def;
    const n = Number(v);
    return Number.isFinite(n) ? n : def;
  };

  return {
    PGHOST: requireEnv("PGHOST")!,
    PGPORT: requireEnv("PGPORT", true),
    PGDATABASE: requireEnv("PGDATABASE")!,
    PGSSLMODE: (Deno.env.get("PGSSLMODE") as "disable" | "require" | "verify-full") ?? "require",
    PGSSLROOTCERT: Deno.env.get("PGSSLROOTCERT"),

    PG_CONNECT_ATTEMPTS: asInt("PG_CONNECT_ATTEMPTS", 3),
    PG_CONNECT_MIN_BACKOFF_MS: asInt("PG_CONNECT_MIN_BACKOFF_MS", 50),
    PG_CONNECT_MAX_BACKOFF_MS: asInt("PG_CONNECT_MAX_BACKOFF_MS", 250),
    PG_CONNECT_TIMEOUT_MS: asInt("PG_CONNECT_TIMEOUT_MS", 3000),
  };
}

export function getClientFactory(
  env: {
    PGHOST: string;
    PGPORT?: string;
    PGDATABASE: string;
    PGSSLMODE?: "disable" | "require" | "verify-full";
    PGSSLROOTCERT?: string;

    // Connect tuning
    PG_CONNECT_ATTEMPTS?: number;
    PG_CONNECT_MIN_BACKOFF_MS?: number;
    PG_CONNECT_MAX_BACKOFF_MS?: number;
    PG_CONNECT_TIMEOUT_MS?: number;
  },
  appName: string | undefined,
  role: RoleCreds,
): () => Promise<DbClient> {
  const hostname = env.PGHOST;
  const database = env.PGDATABASE;
  if (!hostname) throw new Error("PGHOST is required");
  if (!database) throw new Error("PGDATABASE is required");

  const port = Number(env.PGPORT ?? "5432");
  const sslMode = env.PGSSLMODE ?? "require";
  const sslRootCert = env.PGSSLROOTCERT;

  const connectRetry = {
    attempts: env.PG_CONNECT_ATTEMPTS ?? 3,
    minBackoffMs: env.PG_CONNECT_MIN_BACKOFF_MS ?? 50,
    maxBackoffMs: env.PG_CONNECT_MAX_BACKOFF_MS ?? 250,
  };
  const connectTimeoutMs = env.PG_CONNECT_TIMEOUT_MS ?? 3000;

  return () =>
    connectPg({
      hostname,
      port,
      database,
      user: role.user,
      ...(hasPassword(role)
        ? { password: role.password }
        : hasPasswordProvider(role)
        ? { passwordProvider: role.passwordProvider }
        : (() => {
          // Exhaustive guard (should be unreachable)
          throw new Error("invalid_role_credentials");
        })()),
      applicationName: appName,
      sslMode,
      sslRootCert,
      connectRetry,
      connectTimeoutMs,
    });
}
