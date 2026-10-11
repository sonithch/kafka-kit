export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * "Equal jitter": a random delay in [delayMs/2, delayMs]. Keeps a minimum
 * wait while spreading out consumers that failed at the same moment.
 */
export function withJitter(delayMs: number, random: () => number = Math.random): number {
  return delayMs / 2 + random() * (delayMs / 2);
}
