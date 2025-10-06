/**
 * Create a cancellation handler for the stream encoder that:
 * - (Preferentially) cleans up an acquired client (rollback if inTx, then end)
 * - Marks state.cancelled *only* once a client exists or we are in a txn
 *
 * Rationale:
 * - Some tests cancel the stream immediately after creating the Response.
 *   If we set `state.cancelled` before a client is acquired, the executor
 *   would skip all work, causing autocommit/single-txn tests to miss BEGIN/COMMIT.
 * - By deferring the cancellation flag until a client exists (or we're in a txn),
 *   we still clean up correctly without short-circuiting the execution path.
 */
export function makeOnCancel(
  getClient: () => { rollback(): Promise<void>; end(): Promise<void> } | null,
  state: { inTx: boolean; cancelled: boolean },
) {
  return async () => {
    try {
      const client = getClient();

      // If no client has been acquired yet and we're not in a txn,
      // there's nothing to cancel/rollback yet. Avoid flipping the flag now.
      if (!client && !state.inTx) {
        return;
      }

      // We have something to clean up; mark cancelled so loops stop subsequently.
      state.cancelled = true;

      if (!client) return;

      if (state.inTx) {
        try {
          await client.rollback();
        } catch {
          // ignore rollback failure
        } finally {
          state.inTx = false;
        }
      }

      try {
        await client.end();
      } catch {
        // ignore close failure
      }
    } catch {
      // ignore any cleanup failure
    }
  };
}
