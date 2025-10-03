import { Client, type ClientOptions } from "../../../deps.ts";
import { classifyConnectError } from "./errors.ts";
import { jitt, sleep, withTimeout } from "./utils.ts";

async function resolvePassword(
  password?: string,
  passwordProvider?: () => Promise<string> | string,
): Promise<string> {
  if (typeof password === "string") return password;
  if (passwordProvider) {
    const v = await passwordProvider();
    if (!v) throw new Error("empty_password_from_provider");
    return v;
  }
  throw new Error("missing_password");
}

async function connectWithRetries(
  opts: ClientOptions,
  retry?: {
    attempts?: number;
    minBackoffMs?: number;
    maxBackoffMs?: number;
  },
  connectTimeoutMs = 3000,
) {
  const attempts = Math.max(1, retry?.attempts ?? 3);
  const minB = retry?.minBackoffMs ?? 50;
  const maxB = retry?.maxBackoffMs ?? 250;

  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    const client = new Client(opts);
    try {
      await withTimeout(client.connect(), connectTimeoutMs);
      return client;
    } catch (e) {
      try {
        await client.end();
      } catch { /* ignore */ }

      const classified = classifyConnectError(e);
      // Auth errors shouldn't be retried; surface immediately.
      if (classified.status === 401) {
        throw classified;
      }

      lastErr = classified;
      if (i < attempts - 1) {
        await sleep(jitt(minB, maxB));
      }
    }
  }
  throw lastErr;
}

export const ConnectPrimitives = {
  resolvePassword,
  connectWithRetries,
};
