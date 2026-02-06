import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { logger, type LogLevel, type LogSink } from '../../src/infra/logger';

describe('Logger shutdown', () => {
  let prevLevel: LogLevel;
  let prevDisplay: boolean;
  let prevConsoleMode: 'ui' | 'verbose';

  beforeEach(() => {
    prevLevel = logger.getLevel();
    prevDisplay = logger.isDisplayEnabled();
    prevConsoleMode = logger.getConsoleMode();
    logger.setConsoleMode('verbose');
    logger.setLevel('info');
    logger.setDisplay(true);
  });

  afterEach(async () => {
    await logger.close();
    logger.setSinks([]);
    logger.resetSinkToConsole();
    logger.setLevel(prevLevel);
    logger.setDisplay(prevDisplay);
    logger.setConsoleMode(prevConsoleMode);
  });

  it('flush/close are called on sinks', async () => {
    let flushed = 0;
    let closed = 0;
    const sink: LogSink = {
      kind: 'file',
      write: () => {},
      flush: () => {
        flushed += 1;
      },
      close: () => {
        closed += 1;
      },
    };

    logger.setSinks([sink]);
    logger.info('line');
    await logger.flush();
    await logger.close();

    expect(flushed).toBe(1);
    expect(closed).toBe(1);
  });
});
