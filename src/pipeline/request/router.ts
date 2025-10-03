import { methodNotAllowed, notFound } from "../../shared/http.ts";
import { handleBatch } from "./handlers/batch.ts";
import { handleReady } from "./handlers/ready.ts";

export function router(req: Request): Promise<Response> | Response {
  const url = new URL(req.url);

  if (url.pathname === "/batch") {
    if (req.method !== "POST") return methodNotAllowed(["POST"]);
    return handleBatch(req);
  }

  if (url.pathname === "/ready") {
    if (req.method !== "GET") return methodNotAllowed(["GET"]);
    return handleReady(req);
  }

  return notFound();
}
