export type LogMark =
  | 'lifecycle'
  | 'ws'
  | 'connect'
  | 'socket'
  | 'heartbeat'
  | 'cleanup'
  | 'timeout'
  | 'shutdown'
  | 'decision'
  | 'ok'
  | 'warn'
  | 'error'
  | 'risk';

const MARK: Record<LogMark, string> = {
  lifecycle: '🔄',
  ws: '📡',
  connect: '📡',
  socket: '🔌',
  heartbeat: '💓',
  cleanup: '🧹',
  timeout: '⏳',
  shutdown: '🛑',
  decision: '🧠',
  ok: '✅',
  warn: '⚠️',
  error: '❌',
  risk: '🛡️',
};

// Фича-флаг: LOG_MARKERS=on|off|auto (по умолчанию auto: включено в TTY, выключено в CI/pipe)
const raw = (process.env.LOG_MARKERS ?? 'auto').toLowerCase();
const ENABLED = (() => {
  if (['0', 'off', 'false'].includes(raw)) return false;
  if (['1', 'on', 'true'].includes(raw)) return true;
  return Boolean(process.stdout.isTTY) && !process.env.CI;
})();

/**
 * Добавляет эмодзи-маркер в начало сообщения.
 * Возвращает обычную строку, не меняя формат логов.
 */
export function m(mark: LogMark, message: string): string {
  if (!ENABLED) return message;
  const prefix = MARK[mark];
  if (!prefix) return message;
  return `${prefix} ${message}`;
}
