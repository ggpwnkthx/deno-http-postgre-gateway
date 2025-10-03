/**
 * Create a cancellation handler for the stream encoder that:
 * - Marks state.cancelled
 * - Rolls back if we're in a transaction
 * - Ends the client
 *
 * NOTE: The indirection via `getClient()` avoids capturing a stale reference.
 */
export function makeOnCancel(
  getClient: () => { rollback(): Promise<void>; end(): Promise<void> } | null,
  state: { inTx: boolean; cancelled: boolean },
) {
  return async () => {
    state.cancelled = true;
    try {
      const client = getClient();
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
