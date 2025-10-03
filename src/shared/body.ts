const DECODER = new TextDecoder();

/**
 * Read and parse a JSON body with a hard byte cap.
 * - Preallocates exactly content-length when provided and valid (single copy).
 * - Falls back to chunk accumulation when content-length is missing.
 */
export async function readJsonBody<T>(req: Request, maxBytes: number): Promise<T> {
  const clRaw = req.headers.get("content-length");
  const reader = req.body?.getReader();
  if (!reader) throw new Error("missing body");

  // If we have a valid content-length, enforce it strictly and preallocate.
  const cl = clRaw !== null ? Number(clRaw) : null;
  if (cl !== null) {
    if (!Number.isFinite(cl) || cl < 0) throw new Error("invalid_content_length");
    if (cl > maxBytes) throw new Error("request_body_too_large");

    const buf = new Uint8Array(cl);
    let filled = 0;

    while (filled < cl) {
      const { done, value } = await reader.read();
      if (done) break;
      const v = value!;
      const remaining = cl - filled;
      if (v.byteLength > remaining) throw new Error("request_body_too_large");
      buf.set(v, filled);
      filled += v.byteLength;
    }
    if (filled !== cl) throw new Error("invalid_content_length"); // fewer bytes than advertised
    return JSON.parse(DECODER.decode(buf)) as T;
  }

  // No content-length → accumulate chunks but enforce maxBytes.
  let received = 0;
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    const v = value!;
    received += v.byteLength;
    if (received > maxBytes) throw new Error("request_body_too_large");
    chunks.push(v);
  }
  const all = new Uint8Array(received);
  let off = 0;
  for (const c of chunks) {
    all.set(c, off);
    off += c.length;
  }
  return JSON.parse(DECODER.decode(all)) as T;
}
