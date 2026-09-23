export interface Logger {
  info(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, context?: Record<string, unknown>): void;
  debug?(message: string, context?: Record<string, unknown>): void;
}

export function createLogger(service: string, customLogger?: Logger): Logger {
  if (customLogger) return customLogger;

  return {
    info: (msg, ctx) => emit("info", service, msg, ctx),
    warn: (msg, ctx) => emit("warn", service, msg, ctx),
    error: (msg, ctx) => emit("error", service, msg, ctx),
    debug: (msg, ctx) => emit("debug", service, msg, ctx),
  };
}

function emit(level: string, service: string, message: string, context?: Record<string, unknown>): void {
  const entry = {
    level,
    timestamp: new Date().toISOString(),
    service,
    message,
    ...context,
  };
  process.stdout.write(JSON.stringify(entry) + "\n");
}
