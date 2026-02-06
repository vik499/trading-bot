import fs from 'node:fs';
import path from 'node:path';

export interface RotatingFileWriterOptions {
  maxBytes: number;
  maxFiles: number;
}

export class RotatingFileWriter {
  private readonly filePath: string;
  private readonly maxBytes: number;
  private readonly maxFiles: number;
  private currentSize = 0;
  private closed = false;

  constructor(filePath: string, options: RotatingFileWriterOptions) {
    this.filePath = filePath;
    this.maxBytes = Math.max(0, Math.floor(options.maxBytes));
    this.maxFiles = Math.max(0, Math.floor(options.maxFiles));

    this.ensureDir();
    this.currentSize = this.safeStatSize(this.filePath);
  }

  write(line: string): void {
    if (this.closed) return;
    const payload = line.endsWith('\n') ? line : `${line}\n`;
    const bytes = Buffer.byteLength(payload);
    if (this.shouldRotate(bytes)) {
      this.rotate();
    }
    try {
      fs.appendFileSync(this.filePath, payload);
      this.currentSize += bytes;
    } catch {
      // ignore: logging must not crash the process
    }
  }

  flush(): void {
    // sync writes: no buffered data
  }

  close(): void {
    this.closed = true;
  }

  private ensureDir(): void {
    try {
      const dir = path.dirname(this.filePath);
      fs.mkdirSync(dir, { recursive: true });
    } catch {
      // ignore
    }
  }

  private safeStatSize(filePath: string): number {
    try {
      if (!fs.existsSync(filePath)) return 0;
      return fs.statSync(filePath).size;
    } catch {
      return 0;
    }
  }

  private shouldRotate(nextBytes: number): boolean {
    if (this.maxBytes <= 0) return false;
    return this.currentSize + nextBytes > this.maxBytes;
  }

  private rotate(): void {
    if (this.maxBytes <= 0) return;

    if (this.maxFiles <= 0) {
      try {
        fs.writeFileSync(this.filePath, '');
      } catch {
        // ignore
      }
      this.currentSize = 0;
      return;
    }

    try {
      const oldest = `${this.filePath}.${this.maxFiles}`;
      if (fs.existsSync(oldest)) fs.unlinkSync(oldest);

      for (let i = this.maxFiles - 1; i >= 1; i -= 1) {
        const src = `${this.filePath}.${i}`;
        const dest = `${this.filePath}.${i + 1}`;
        if (fs.existsSync(src)) fs.renameSync(src, dest);
      }

      if (fs.existsSync(this.filePath)) fs.renameSync(this.filePath, `${this.filePath}.1`);
    } catch {
      // ignore
    }

    this.currentSize = 0;
  }
}
