import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const roots = [
  path.join(__dirname, '..', 'src', 'analytics'),
  path.join(__dirname, '..', 'src', 'globalData'),
  path.join(__dirname, '..', 'src', 'research'),
  path.join(__dirname, '..', 'src', 'strategy'),
  path.join(__dirname, '..', 'src', 'risk'),
  path.join(__dirname, '..', 'src', 'execution'),
  path.join(__dirname, '..', 'src', 'portfolio'),
  path.join(__dirname, '..', 'src', 'metrics'),
];

function listTsFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) files.push(...listTsFiles(full));
    else if (e.isFile() && full.endsWith('.ts')) files.push(full);
  }
  return files;
}

describe('Meta determinism guard', () => {
  it('rejects payload.meta reads in replay-sensitive planes', () => {
    const pattern = /payload\s*\.\s*meta|payload\?\.meta|\.payload\.meta/;
    const violations: Array<{ file: string; line: number; lineText: string }> = [];

    for (const root of roots) {
      for (const file of listTsFiles(root)) {
        const content = fs.readFileSync(file, 'utf8');
        const lines = content.split(/\r?\n/);
        lines.forEach((lineText, idx) => {
          if (pattern.test(lineText)) {
            violations.push({ file, line: idx + 1, lineText: lineText.trim() });
          }
        });
      }
    }

    const message = violations
      .map((v) => `${path.relative(path.join(__dirname, '..'), v.file)}:${v.line} -> ${v.lineText}`)
      .join('\n');
    expect(violations, message || 'No payload.meta usage in replay-sensitive planes').toHaveLength(0);
  });

  it('rejects Date.now usage in replay-sensitive planes', () => {
    const pattern = /\bDate\.now\(/;
    const violations: Array<{ file: string; line: number; lineText: string }> = [];

    for (const root of roots) {
      for (const file of listTsFiles(root)) {
        const content = fs.readFileSync(file, 'utf8');
        const lines = content.split(/\r?\n/);
        lines.forEach((lineText, idx) => {
          if (pattern.test(lineText)) {
            violations.push({ file, line: idx + 1, lineText: lineText.trim() });
          }
        });
      }
    }

    const message = violations
      .map((v) => `${path.relative(path.join(__dirname, '..'), v.file)}:${v.line} -> ${v.lineText}`)
      .join('\n');
    expect(violations, message || 'No Date.now usage in replay-sensitive planes').toHaveLength(0);
  });
});
