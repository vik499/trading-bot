import type { LogEntry, LogLevel, LogSink } from '../../infra/logger';
import { RotatingFileWriter } from './RotatingFileWriter';

const ALL_LEVELS: LogLevel[] = ['debug', 'info', 'warn', 'error'];

export interface FileSinkOptions {
  levels?: LogLevel[];
  runId: string;
}

export class FileSink implements LogSink {
  readonly kind = 'file' as const;
  private readonly levels: Set<LogLevel>;
  private readonly runId: string;
  private readonly writer: RotatingFileWriter;

  constructor(writer: RotatingFileWriter, options: FileSinkOptions) {
    this.writer = writer;
    this.levels = new Set(options.levels ?? ALL_LEVELS);
    this.runId = options.runId;
  }

  write(entry: LogEntry, _formatted?: string): void {
    if (!this.levels.has(entry.level)) return;
    const safeMessage = entry.message.replace(/\r?\n/g, '\\n');
    const line = `[${entry.iso}] ${entry.level.toUpperCase()} runId=${this.runId} ${safeMessage}`;
    this.writer.write(line);
  }

  flush(): void {
    this.writer.flush();
  }

  close(): void {
    this.writer.close();
  }
}
