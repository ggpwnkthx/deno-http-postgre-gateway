import type { ClientOptions } from "../../../deps.ts";

export function buildTls(
  mode: "disable" | "require" | "verify-full" | undefined,
  sslRootCert?: string,
): ClientOptions["tls"] | undefined {
  const m = mode ?? "require";
  if (m === "disable") return undefined;
  if (m === "require") return { enforce: true };
  if (m === "verify-full") {
    if (!sslRootCert) throw new Error("PGSSLROOTCERT is required for verify-full");
    return { enforce: true, caCertificates: [sslRootCert] };
  }
  return undefined;
}
