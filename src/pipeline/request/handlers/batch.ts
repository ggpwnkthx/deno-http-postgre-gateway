import { DEFAULTS, HEADER } from "../../../shared/constants.ts";
import { parseAuth } from "../../parse/auth.ts";
import {
  getBooleanHeader,
  getHeaderNormalized,
  getResponseFormat,
  parseStreamingKnobs,
  parseTimeouts,
} from "../../parse/headers.ts";
import { parseBatchRequest } from "../../parse/batch.ts";
import { AppError, badRequest, normalizeError, unprocessable } from "../../../shared/errors.ts";
import { getClientFactory, readDbEnv } from "../../plan/build.ts";
import type { Plan } from "../../../shared/types.ts";
import { executePlan } from "../../execute/executor/runner.ts";
import { envBool } from "../../../shared/env.ts";
import { classifyConnectError } from "../../execute/db/errors.ts";
import { seemsSelectLike } from "../../execute/sql/seems_select_like.ts";
import { detectPgBouncerUnsafe } from "../../execute/sql/pgbouncer.ts";
import { createPerfCtx } from "../../../shared/perf.ts";

function maybeMessage(e: unknown): string | undefined {
  if (e instanceof Error) return e.message;
  if (typeof e === "object" && e !== null) {
    const m = (e as { message?: unknown }).message;
    if (typeof m === "string") return m;
  }
  return undefined;
}

export async function handleBatch(req: Request): Promise<Response> {
  // Use inbound request id if provided, else generate a new one.
  const inboundId = getHeaderNormalized(req.headers, "x-request-id");
  const requestId = inboundId || crypto.randomUUID();

  // Per-request perf toggle: header OR env.
  const perfHeader = getBooleanHeader(req.headers, HEADER.PERF_DEBUG);
  const perf = createPerfCtx({
    enabled: perfHeader ?? envBool("PERF_DEBUG", false),
    requestId,
    minDurationMs: Number(Deno.env.get("PERF_MIN_MS") ?? 0),
  });

  const spanReq = perf.span("request");

  try {
    // PARSE: auth + body + headers
    const spanParse = perf.span("request.parse");
    const auth = parseAuth(req.headers);
    const { statements } = await parseBatchRequest(
      req,
      DEFAULTS.MAX_STATEMENTS,
      DEFAULTS.MAX_SQL_LENGTH,
    );
    const { fetchSize, flushEvery, flushIntervalMs } = parseStreamingKnobs(req.headers);
    const responseFormat = getResponseFormat(req.headers, req.headers.get("accept"));
    const appName = req.headers.get(HEADER.APP_NAME) ?? undefined;
    const timeouts = parseTimeouts(req.headers);
    spanParse.end({ statements: statements.length, fetchSize, responseFormat });

    // Validate per-index header keys are within statements range
    const stmtCount = statements.length;
    const badKeys: Array<{ header: string; index: number }> = [];
    for (const [k] of Object.entries(timeouts.perIndexStatement ?? {})) {
      const idx = Number(k);
      if (!Number.isInteger(idx) || idx < 0 || idx >= stmtCount) {
        badKeys.push({ header: HEADER.STMT_TIMEOUT_PER_INDEX, index: idx });
      }
    }
    for (const [k] of Object.entries(timeouts.perIndexLock ?? {})) {
      const idx = Number(k);
      if (!Number.isInteger(idx) || idx < 0 || idx >= stmtCount) {
        badKeys.push({ header: HEADER.LOCK_TIMEOUT_PER_INDEX, index: idx });
      }
    }
    if (badKeys.length > 0) {
      throw badRequest("per_index_timeout_index_out_of_range", { badKeys, statements: stmtCount });
    }

    // PLAN: environment + db client factory
    const spanPlan = perf.span("request.plan");
    const env = readDbEnv();
    const factory = getClientFactory(env, appName, {
      user: auth.dbUser,
      password: auth.dbPassword,
    });
    spanPlan.end({ host: env.PGHOST, db: env.PGDATABASE });

    // Read-only mode: environment + request header
    const envReadOnly = envBool("READ_ONLY", false);
    const headerAllow = getBooleanHeader(req.headers, HEADER.ALLOW_MUTATIONS);
    const allowMutations = envReadOnly ? (headerAllow ?? false) : (headerAllow ?? true);

    // Compute readOnlyBatch up-front. If mutations aren't allowed, reject early.
    const readOnlyBatch = statements.every((s) => seemsSelectLike(s.sql));
    if (!allowMutations && !readOnlyBatch) {
      const violating: number[] = [];
      statements.forEach((s, i) => {
        if (!seemsSelectLike(s.sql)) violating.push(i);
      });
      throw unprocessable("mutations_not_allowed", { violating });
    }

    // PgBouncer strict: reject unsafe/session features up-front if enabled
    // Enable by default if PGBOUNCER_MODE is "transaction" or "statement".
    const mode = (Deno.env.get("PGBOUNCER_MODE") ?? "").trim().toLowerCase();
    const strictDefault = mode === "transaction" || mode === "statement";
    const pgbouncerStrict = envBool("PGBOUNCER_STRICT", strictDefault);

    if (pgbouncerStrict) {
      const violating: Array<{ index: number; cmd: string }> = [];
      statements.forEach((s, i) => {
        const bad = detectPgBouncerUnsafe(s.sql);
        if (bad.length > 0) violating.push({ index: i, cmd: bad[0] });
      });
      if (violating.length > 0) {
        throw unprocessable("pgbouncer_unsafe_statements", { violating });
      }
    }

    const autocommitReadOnly = envBool("AUTOCOMMIT_READONLY", true);

    // --- Pre-connect to shape connection/login errors BEFORE streaming.
    let preClient: ReturnType<Plan["getClient"]> extends Promise<infer C> ? C : never;
    try {
      const spanConnect = perf.span("request.preconnect");
      preClient = await factory(); // Do the real connect once, here.
      spanConnect.end();
    } catch (e) {
      // Avoid re-classifying if we already have a normalized AppError
      const appErr = e instanceof AppError ? e : classifyConnectError(e);
      const { status, body } = normalizeError(appErr);
      const responseBody = {
        ...body,
        hint: appErr.status === 401 ? "Check DB user/password (or create the role)." : undefined,
      };
      spanReq.end({ error: body.message });
      return new Response(JSON.stringify(responseBody) + "\n", {
        status,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
          "x-request-id": requestId,
        },
      });
    }

    // getClient that returns the preconnected client so executor doesn't reconnect.
    const getClient = () => Promise.resolve(preClient);

    const plan: Plan = {
      id: requestId,
      appName,
      dbUser: auth.dbUser,
      dbHost: env.PGHOST,
      dbName: env.PGDATABASE,
      fetchSize,
      flushEvery,
      flushIntervalMs,
      responseFormat,
      timeouts,
      statements,
      getClient,
      // feature flags
      allowMutations,
      readOnlyBatch,
      autocommitReadOnly,
      // perf ctx
      perf,
    };

    // EXECUTE → STREAM (the executor will close the client in its finally {})
    const res = executePlan(plan);
    spanReq.end();
    return res;
  } catch (e) {
    const { status, body } = normalizeError(e);
    const base: { error: string; message: string; details?: unknown | null } = {
      error: body.error,
      message: body.message,
      details: "details" in body ? body.details : undefined,
    };
    const shouldHint = status === 401 && /missing/i.test(maybeMessage(e) ?? "");
    const responseBody = shouldHint
      ? { ...base, hint: "Provide Authorization header (Basic or Bearer + X-DB-User)" }
      : base;
    spanReq.end({ error: base.message });
    return new Response(JSON.stringify(responseBody) + "\n", {
      status,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "x-request-id": requestId,
      },
    });
  }
}
