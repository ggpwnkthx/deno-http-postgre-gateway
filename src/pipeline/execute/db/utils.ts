export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function jitt(min: number, max: number) {
  const lo = Math.max(0, Math.min(min, max));
  const hi = Math.max(min, max);
  return Math.floor(lo + Math.random() * (hi - lo + 1));
}

/**
 * Race a promise against a timeout.
 *
 * Behavior:
 * - If the timeout elapses, we reject immediately with `${name}_timeout`.
 * - We always clear the timer when either side settles.
 * - We also "unref" the timer (when supported) to avoid test runner leak warnings.
 *
 * Notes:
 * - This avoids the bug where the original promise resolves after the timeout
 *   and "wins" due to a late reject inside a `finally` handler.
 */
export function withTimeout<T>(p: Promise<T>, ms: number, name = "connect"): Promise<T> {
  if (ms <= 0 || !Number.isFinite(ms)) return p;

  let timerId: number | null = null;

  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    // Start the timeout and unref it so it doesn't hold the event loop open or
    // trigger Deno's leak detector if the test finishes very quickly.
    timerId = setTimeout(() => reject(new Error(`${name}_timeout`)), ms) as unknown as number;
    try {
      // Deno 2.x provides unrefTimer; guard for portability.
      (Deno as unknown as { unrefTimer?: (id: number) => void }).unrefTimer?.(timerId!);
    } catch {
      // ignore if not available
    }
  });

  // Whichever settles first wins; always clear the timer.
  return Promise.race([p, timeoutPromise]).finally(() => {
    if (timerId !== null) {
      clearTimeout(timerId);
    }
  });
}
