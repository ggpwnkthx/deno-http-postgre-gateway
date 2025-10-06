// Utilities to consume ReadableStream<Uint8Array> in tests.
export async function streamToU8(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
      total += value.byteLength;
    }
  }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

export async function streamToText(stream: ReadableStream<Uint8Array>): Promise<string> {
  const u8 = await streamToU8(stream);
  return new TextDecoder().decode(u8);
}
