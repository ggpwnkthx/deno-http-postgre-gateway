export interface StreamEncoder {
  stream: ReadableStream<Uint8Array>;
  emit: (type: string, payload: unknown) => void;
  /**
   * Fast path for very high-throughput row streaming:
   * accepts a pre-serialized JSON object (no trailing newline)
   * and wraps it with the statement index + line framing.
   */
  emitRow?(statementIndex: number, rowJson: string): void;
  close(): void;
}
