export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export function jitt(min: number, max: number) {
  const lo = Math.max(0, Math.min(min, max));
  const hi = Math.max(min, max);
  return Math.floor(lo + Math.random() * (hi - lo + 1));
}

export function withTimeout<T>(p: Promise<T>, ms: number, name = "connect") {
  if (ms <= 0 || !Number.isFinite(ms)) return p;
  return Promise.race([
    p,
    new Promise<T>((_res, rej) => {
      const t = setTimeout(() => rej(new Error(`${name}_timeout`)), ms);
      p.finally(() => clearTimeout(t));
    }),
  ]);
}
