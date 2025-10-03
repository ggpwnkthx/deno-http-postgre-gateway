import type { DbClient } from "../../../shared/types.ts";
import { buildTls } from "./tls.ts";
import { ConnectPrimitives } from "./connect.ts";
import type { ClientOptions } from "../../../deps.ts";
import { toDbClientAPI } from "./client_impl.ts";

interface ConnectOptions {
  hostname: string;
  port: number;
  database: string;
  user: string;

  /** Static password (use this OR passwordProvider) */
  password?: string;
  /** Lazy password provider for short-lived tokens (IAM, etc.) */
  passwordProvider?: () => Promise<string> | string;
  applicationName?: string;

  /**
   * SSL mode for connecting through PgBouncer or directly to Postgres.
   * - In most managed environments through PgBouncer, "require" is fine.
   * - "verify-full" requires PGSSLROOTCERT to be provided.
   */
  sslMode?: "disable" | "require" | "verify-full";
  sslRootCert?: string;

  /**
   * Small connect retry budget to smooth serverless bursts against the pool.
   * Defaults: { attempts: 3, minBackoffMs: 50, maxBackoffMs: 250 }
   */
  connectRetry?: {
    attempts?: number;
    minBackoffMs?: number;
    maxBackoffMs?: number;
  };

  /** Hard wall for a single connect attempt. Default 3000ms. */
  connectTimeoutMs?: number;
}

export async function connectPg(opts: ConnectOptions): Promise<DbClient> {
  const tls: ClientOptions["tls"] | undefined = buildTls(opts.sslMode, opts.sslRootCert);
  const password = await ConnectPrimitives.resolvePassword(opts.password, opts.passwordProvider);

  const client = await ConnectPrimitives.connectWithRetries(
    {
      hostname: opts.hostname,
      port: opts.port,
      database: opts.database,
      user: opts.user,
      password,
      applicationName: opts.applicationName,
      tls,
    },
    opts.connectRetry,
    opts.connectTimeoutMs ?? 3000,
  );

  return toDbClientAPI(client);
}
