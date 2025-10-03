import { AppError, unauthorized } from "../../../shared/errors.ts";

/** Narrow unknown to a generic object map. */
function asRecord(v: unknown): Record<string, unknown> | null {
  return (typeof v === "object" && v !== null) ? (v as Record<string, unknown>) : null;
}

/** Extract best-effort PG code + message from driver errors. */
function extractPgError(e: unknown): { code: string | null; message: string } {
  const obj = asRecord(e);
  let code: string | null = null;
  let message = "db_error";

  if (obj) {
    if (typeof obj.code === "string") code = obj.code;
    const fields = asRecord(obj.fields);
    if (!code && fields && typeof fields.code === "string") {
      code = fields.code;
    }
    if (typeof obj.message === "string") {
      message = obj.message;
    } else {
      message = String(e ?? "db_error");
    }
  } else {
    message = String(e ?? "db_error");
  }

  return { code, message };
}

/** Heuristics for connect-time auth-ish failures → map to 401. */
function isAuthConnectError(e: unknown): boolean {
  const { code, message } = extractPgError(e);
  const msg = message.toLowerCase();

  // SQLSTATEs commonly tied to auth/role failures
  // 28P01 invalid_password, 28000 invalid_authorization_specification,
  // 42501 insufficient_privilege (treat as auth-ish), 42704 undefined_object (role does not exist)
  if (code && ["28p01", "28000", "42501", "42704"].includes(code.toLowerCase())) return true;

  // Message text fallbacks
  if (msg.includes("password authentication failed")) return true;
  if (msg.includes("no pg_hba.conf entry")) return true;
  if (msg.includes("given role does not exist")) return true;
  if (msg.includes("role") && msg.includes("does not exist")) return true;
  if (msg.includes("authentication failed")) return true;

  return false;
}

/** Heuristics for unavailability (timeouts/refused) → map to 503. */
function isUnavailableConnectError(e: unknown): boolean {
  const { message } = extractPgError(e);
  const m = message.toLowerCase();
  return (
    /timeout|timed out/.test(m) ||
    /econnrefused/.test(m) ||
    /could not connect/.test(m) ||
    /connection refused/.test(m)
  );
}

/** Normalize a connect error into a consistent AppError (401/503/500). */
export function classifyConnectError(e: unknown): AppError {
  const { code, message } = extractPgError(e);

  if (isAuthConnectError(e)) {
    return unauthorized("db_auth_failed", { code, message });
  }
  if (isUnavailableConnectError(e)) {
    return new AppError(503, "db_unavailable", message, { code, message });
  }
  return new AppError(500, "internal_error", message, { code, message });
}
