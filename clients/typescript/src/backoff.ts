/**
 * Exponential backoff with full jitter.
 *
 * Jitter matters more than the exponent here: without it, every client
 * disconnected by a deploy retries on the same schedule and stampedes the hub
 * the moment it comes back.
 */
export function backoffDelay(
  attempt: number,
  minMs: number,
  maxMs: number,
): number {
  const exp = Math.min(maxMs, minMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(Math.random() * exp);
}
