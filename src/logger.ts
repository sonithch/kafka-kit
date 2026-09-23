export interface Logger {
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  debug?(message: string, context?: Record<string, unknown>): void;
}

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 } as const;
type Level = keyof typeof LEVELS;

function currentThreshold(): number {
  const configured = (process.env.LOG_LEVEL ?? "info").toLowerCase() as Level;
  return LEVELS[configured] ?? LEVELS.info;
}

export function createLogger(service: string, customLogger?: Logger): Logger {
  if (customLogger) return customLogger;

  const threshold = currentThreshold();

  return {
    info: (msg, ctx) => emit("info", threshold, service, msg, ctx),
    warn: (msg, ctx) => emit("warn", threshold, service, msg, ctx),
    error: (msg, ctx) => emit("error", threshold, service, msg, ctx),
    debug: (msg, ctx) => emit("debug", threshold, service, msg, ctx),
  };
}

function emit(
  level: Exclude<Level, "silent">,
  threshold: number,
  service: string,
  message: string,
  context?: Record<string, unknown>
): void {
  if (LEVELS[level] < threshold) return;
  const entry = {
    level,
    timestamp: new Date().toISOString(),
    service,
    message,
    ...context,
  };
  process.stdout.write(JSON.stringify(entry) + "\n");
}
