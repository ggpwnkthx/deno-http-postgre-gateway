export interface StreamEncoder {
  stream: ReadableStream<Uint8Array>;
  emit: (type: string, payload: unknown) => void;

  /**
   * Fast path for very high-throughput row streaming:
   * accepts a pre-serialized JSON object (no trailing newline)
   * and wraps it with the statement index + line framing.
   *
   * Text encoders (NDJSON/SSE) typically implement this.
   */
  emitRow?(statementIndex: number, rowJson: string): void;

  /**
   * Array-row fast path: row is an ordered array of column values.
   * Encoders that benefit from avoiding per-row POJO creation should implement this.
   */
  emitArrayRow?(statementIndex: number, row: unknown[]): void;

  /**
   * Optional schema hint (per statement) for array/object modes.
   * Called before any rows for that statement.
   */
  onSchema?(statementIndex: number, columns: string[]): void;

  /**
   * If true, the executor will request array rows (most efficient).
   * If falsey, and preferObjects === true, the executor will request objects.
   * Otherwise (default), it will request pre-serialized JSON for text encoders.
   */
  preferArrays?: boolean;

  /**
   * If true (and preferArrays !== true), the executor will request and deliver **object rows**
   * to this encoder via `emit('row', { statement, row })`.
   *
   * Binary/object encoders (CSV/MsgPack/CBOR/Arrow) can set this to true when they do not
   * implement preferArrays.
   */
  preferObjects?: boolean;

  close(): void;
}
