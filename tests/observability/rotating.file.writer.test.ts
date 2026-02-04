import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { RotatingFileWriter } from '../../src/observability/logger/RotatingFileWriter';

describe('RotatingFileWriter', () => {
  it('rotates when size exceeded and ignores writes after close', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bot-rotate-'));
    const filePath = path.join(dir, 'app.log');
    const writer = new RotatingFileWriter(filePath, { maxBytes: 50, maxFiles: 2 });

    writer.write('a'.repeat(40));
    writer.write('b'.repeat(40));
    writer.write('c'.repeat(40));
    writer.write('d'.repeat(40));

    expect(fs.existsSync(`${filePath}.1`)).toBe(true);
    expect(fs.existsSync(`${filePath}.2`)).toBe(true);
    expect(fs.existsSync(`${filePath}.3`)).toBe(false);

    const sizeBefore = fs.readFileSync(filePath, 'utf8').length;
    writer.close();
    writer.write('c'.repeat(40));
    const sizeAfter = fs.readFileSync(filePath, 'utf8').length;

    expect(sizeAfter).toBe(sizeBefore);
  });
});
