export class AppError extends Error {
  status: number;
  code: string;
  details?: unknown;
  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export const badRequest = (m: string, d?: unknown) => new AppError(400, "bad_request", m, d);
export const unauthorized = (m = "missing_or_invalid_auth", d?: unknown) =>
  new AppError(401, "unauthorized", m, d);
export const unprocessable = (m: string, d?: unknown) => new AppError(422, "unprocessable", m, d);
export const tooMany = (m: string, d?: unknown) => new AppError(429, "too_many", m, d);

/** Shape compatible with routes for consistent error responses. */
export function normalizeError(e: unknown) {
  if (e instanceof AppError) {
    return {
      status: e.status,
      body: { error: e.code, message: e.message, details: e.details ?? null },
    };
  }
  const msg = e instanceof Error ? e.message : String(e);
  return { status: 500, body: { error: "internal_error", message: msg } };
}
