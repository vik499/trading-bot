import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { logger, type LogLevel } from '../../src/infra/logger';
import { FileSink } from '../../src/observability/logger/FileSink';
import { RotatingFileWriter } from '../../src/observability/logger/RotatingFileWriter';

describe('FileSink routing', () => {
  let prevLevel: LogLevel;
  let prevDisplay: boolean;
  let prevConsoleMode: 'ui' | 'verbose';

  beforeEach(() => {
    prevLevel = logger.getLevel();
    prevDisplay = logger.isDisplayEnabled();
    prevConsoleMode = logger.getConsoleMode();
    logger.setConsoleMode('verbose');
    logger.setLevel('debug');
    logger.setDisplay(false);
  });

  afterEach(async () => {
    await logger.close();
    logger.setSinks([]);
    logger.resetSinkToConsole();
    logger.setLevel(prevLevel);
    logger.setDisplay(prevDisplay);
    logger.setConsoleMode(prevConsoleMode);
  });

  it('routes levels to correct files and includes runId', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bot-logs-'));
    const runId = 'test-run';

    const appPath = path.join(dir, 'app.log');
    const warnPath = path.join(dir, 'warnings.log');
    const errorPath = path.join(dir, 'errors.log');

    logger.setSinks([
      new FileSink(new RotatingFileWriter(appPath, { maxBytes: 1_000_000, maxFiles: 2 }), { runId }),
      // warnings.log: WARN + ERROR
      new FileSink(new RotatingFileWriter(warnPath, { maxBytes: 1_000_000, maxFiles: 2 }), {
        runId,
        levels: ['warn', 'error'],
      }),
      new FileSink(new RotatingFileWriter(errorPath, { maxBytes: 1_000_000, maxFiles: 2 }), {
        runId,
        levels: ['error'],
      }),
    ]);

    logger.debug('debug-msg');
    logger.info('info-msg');
    logger.warn('warn-msg');
    logger.error('error-msg');

    const readLines = (filePath: string): string[] => {
      if (!fs.existsSync(filePath)) return [];
      return fs
        .readFileSync(filePath, 'utf8')
        .split('\n')
        .filter((line) => line.trim().length > 0);
    };

    const appLines = readLines(appPath);
    const warnLines = readLines(warnPath);
    const errorLines = readLines(errorPath);

    expect(appLines).toHaveLength(4);
    expect(warnLines).toHaveLength(2);
    expect(errorLines).toHaveLength(1);

    for (const line of [...appLines, ...warnLines, ...errorLines]) {
      expect(line).toContain(`runId=${runId}`);
    }

    const consoleLines: string[] = [];
    logger.setSink((_entry, formatted) => {
      consoleLines.push(formatted);
    });
    logger.setDisplay(true);
    logger.info('info-msg-2');

    const appLinesAfter = readLines(appPath);
    expect(appLinesAfter).toHaveLength(5);
    expect(consoleLines).toHaveLength(1);
  });
});
