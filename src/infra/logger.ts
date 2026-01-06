// Централизованный модуль логирования.
// Цели (production-minded, но без фанатизма):
// - уровни логов (debug/info/warn/error)
// - история (ring buffer), чтобы можно было показать tail
// - управляемый вывод (можно включать/выключать отображение, не теряя историю)
// - троттлинг для шумных событий (например, тикер)
//
// Важно: логгер — это сервис. Он хранит историю и решает, что выводить.
// CLI позже сможет:
// - выключать отображение логов, но сохранять историю
// - показывать tail
// - менять уровень
// - подключить свой sink (например, чтобы не ломать prompt)

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogEntry {
  ts: number;              // unix ms
  iso: string;             // ISO time
  level: LogLevel;
  message: string;
}

type LogSink = (entry: LogEntry, formatted: string) => void;

const LEVEL_WEIGHT: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const now = (): number => Date.now();
const iso = (): string => {
  const d = new Date();
  const pad = (v: number, len = 2) => String(Math.abs(v)).padStart(len, '0');
  const tzMinutes = -d.getTimezoneOffset();
  const sign = tzMinutes >= 0 ? '+' : '-';
  const tzH = pad(Math.floor(Math.abs(tzMinutes) / 60));
  const tzM = pad(Math.abs(tzMinutes) % 60);

  // ISO-подобный формат в системной тайм-зоне: 2026-01-06T15:04:05.123+03:00
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}${sign}${tzH}:${tzM}`;
};

function formatEntry(entry: LogEntry): string {
  // Формат оставляем “похожим на текущий”, но делаем стабильным и машинно-парсимым.
  // Если захочешь позже, можно добавить JSON-логирование.
  return `[${entry.iso}] ${entry.level.toUpperCase()}: ${entry.message}`;
}

class Logger {
  private minLevel: LogLevel = 'info';
  private displayEnabled = true;

  // История логов (ring buffer)
  private readonly buffer: LogEntry[] = [];
  private bufferLimit = 2000;

  // Троттлинг по ключам (например: "ticker:BTCUSDT")
  private readonly lastByKey = new Map<string, number>();

  // Sink по умолчанию пишет в stdout/stderr.
  private sink: LogSink = (entry, formatted) => {
    if (entry.level === 'error') console.error(formatted);
    else console.log(formatted);
  };

  // --------------- публичный API управления ---------------

  /** Включить/выключить отображение логов в консоли. История при этом сохраняется. */
  setDisplay(enabled: boolean): void {
    this.displayEnabled = enabled;
  }

  /** Текущий флаг отображения. */
  isDisplayEnabled(): boolean {
    return this.displayEnabled;
  }

  /** Установить минимальный уровень отображения. */
  setLevel(level: LogLevel): void {
    this.minLevel = level;
  }

  getLevel(): LogLevel {
    return this.minLevel;
  }

  /** Настроить лимит истории (кольцевого буфера). */
  setHistoryLimit(limit: number): void {
    // защита от странных значений
    this.bufferLimit = Math.max(100, Math.floor(limit));
    this.trimBuffer();
  }

  getHistoryLimit(): number {
    return this.bufferLimit;
  }

  /** Вернуть последние N записей истории (для команды logs tail N). */
  tail(count = 50): LogEntry[] {
    const n = Math.max(0, Math.floor(count));
    return this.buffer.slice(Math.max(0, this.buffer.length - n));
  }

  /** Вернуть всю историю (используй осторожно). */
  getHistory(): LogEntry[] {
    return [...this.buffer];
  }

  /**
   * Заменить sink (куда реально выводить лог).
   * CLI позже сможет поставить sink, который не ломает prompt.
   */
  setSink(sink: LogSink): void {
    this.sink = sink;
  }

  resetSinkToConsole(): void {
    this.sink = (entry, formatted) => {
      if (entry.level === 'error') console.error(formatted);
      else console.log(formatted);
    };
  }

  // --------------- лог-методы ---------------

  debug(msg: string): void {
    this.write('debug', msg);
  }

  info(msg: string): void {
    this.write('info', msg);
  }

  warn(msg: string): void {
    this.write('warn', msg);
  }

  error(msg: string, err?: unknown): void {
    // Стака не надо на весь экран: коротко и по делу.
    if (err instanceof Error) {
      const shortStack = err.stack?.split('\n').slice(0, 4).join('\n');
      this.write('error', `${msg}\n${shortStack ?? ''}`.trim());
      return;
    }
    this.write('error', msg);
  }

  /**
   * Троттлинг: пропускаем лог не чаще, чем раз в intervalMs для одного key.
   * Пример: logger.infoThrottled('ticker:BTCUSDT', '...', 3000)
   */
  infoThrottled(key: string, msg: string, intervalMs: number): void {
    if (this.shouldAllowByKey(key, intervalMs)) this.write('info', msg);
  }

  debugThrottled(key: string, msg: string, intervalMs: number): void {
    if (this.shouldAllowByKey(key, intervalMs)) this.write('debug', msg);
  }

  // --------------- внутренности ---------------

  private write(level: LogLevel, message: string): void {
    const entry: LogEntry = {
      ts: now(),
      iso: iso(),
      level,
      message,
    };

    // 1) всегда сохраняем историю
    this.buffer.push(entry);
    this.trimBuffer();

    // 2) решаем, показывать ли
    if (!this.displayEnabled) return;
    if (LEVEL_WEIGHT[level] < LEVEL_WEIGHT[this.minLevel]) return;

    // 3) выводим через sink
    const formatted = formatEntry(entry);
    this.sink(entry, formatted);
  }

  private trimBuffer(): void {
    const overflow = this.buffer.length - this.bufferLimit;
    if (overflow > 0) this.buffer.splice(0, overflow);
  }

  private shouldAllowByKey(key: string, intervalMs: number): boolean {
    const ms = Math.max(0, Math.floor(intervalMs));
    const last = this.lastByKey.get(key) ?? 0;
    const t = now();
    if (t - last < ms) return false;
    this.lastByKey.set(key, t);
    return true;
  }
}

// Экспортируем единый экземпляр.
// Если позже потребуется DI/тесты, можно будет экспортировать фабрику.
export const logger = new Logger();