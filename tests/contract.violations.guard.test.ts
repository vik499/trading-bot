import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';

const SRC_ROOT = path.join(__dirname, '..', 'src');
const EVENTBUS_PATH = path.join(SRC_ROOT, 'core', 'events', 'EventBus.ts');

function listFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...listFiles(full));
    else if (entry.isFile() && full.endsWith('.ts')) files.push(full);
  }
  return files;
}

function extractBotEventTopics(source: string): Set<string> {
  const start = source.indexOf('export type BotEventMap');
  if (start === -1) return new Set();
  const end = source.indexOf('};', start);
  const slice = end === -1 ? source.slice(start) : source.slice(start, end);
  const regex = /['"]([^'"]+)['"]\s*:\s*\[/g;
  const topics = new Set<string>();
  let match: RegExpExecArray | null;
  while ((match = regex.exec(slice))) {
    topics.add(match[1]);
  }
  return topics;
}

function extractUsedTopics(content: string): Array<{ topic: string; index: number }> {
  const matches: Array<{ topic: string; index: number }> = [];
  const publishRegex = /\.publish\(\s*['"]([^'"]+)['"]/g;
  const subscribeRegex = /\.subscribe\(\s*['"]([^'"]+)['"]/g;
  const onRegex = /eventBus\.on\(\s*['"]([^'"]+)['"]/g;
  let match: RegExpExecArray | null;

  while ((match = publishRegex.exec(content))) {
    matches.push({ topic: match[1], index: match.index });
  }
  while ((match = subscribeRegex.exec(content))) {
    matches.push({ topic: match[1], index: match.index });
  }
  while ((match = onRegex.exec(content))) {
    matches.push({ topic: match[1], index: match.index });
  }

  return matches;
}

function lineNumberAt(content: string, index: number): number {
  return content.slice(0, index).split(/\r?\n/).length;
}

describe('Contract guard: all used topics exist in BotEventMap', () => {
  it('rejects unknown topics', () => {
    const eventBusSource = fs.readFileSync(EVENTBUS_PATH, 'utf8');
    const knownTopics = extractBotEventTopics(eventBusSource);

    const violations: Array<{ file: string; line: number; topic: string }> = [];

    for (const file of listFiles(SRC_ROOT)) {
      const content = fs.readFileSync(file, 'utf8');
      for (const match of extractUsedTopics(content)) {
        if (!knownTopics.has(match.topic)) {
          violations.push({ file, line: lineNumberAt(content, match.index), topic: match.topic });
        }
      }
    }

    const message = violations
      .map((v) => `${path.relative(path.join(__dirname, '..'), v.file)}:${v.line} -> ${v.topic}`)
      .join('\n');
    expect(violations, message || 'All topics are defined in BotEventMap').toHaveLength(0);
  });
});
