import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createMeta } from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { GlobalDataQualityMonitor } from '../../src/globalData/GlobalDataQualityMonitor';
import { logger, type LogLevel } from '../../src/infra/logger';
import { formatDataSourceDegradedUiLine } from '../../src/observability/ui/dataQualityUi';

describe('DataQuality key consistency', () => {
  let prevLevel: LogLevel;
  let prevDisplay: boolean;
  let prevConsoleMode: 'ui' | 'verbose';

  beforeEach(() => {
    prevLevel = logger.getLevel();
    prevDisplay = logger.isDisplayEnabled();
    prevConsoleMode = logger.getConsoleMode();
    logger.setLevel('debug');
    logger.setDisplay(false);
    logger.setConsoleMode('verbose');
  });

  afterEach(async () => {
    await logger.close();
    logger.setSinks([]);
    logger.resetSinkToConsole();
    logger.setLevel(prevLevel);
    logger.setDisplay(prevDisplay);
    logger.setConsoleMode(prevConsoleMode);
  });

  it('uses the same key string in stale warnings and console UI lines', () => {
    const lines: string[] = [];
    logger.setSinks([
      {
        write: (_entry, formatted) => {
          lines.push(formatted);
        },
      },
    ]);

    const bus = createTestEventBus();
    const monitor = new GlobalDataQualityMonitor(bus, {
      expectedIntervalMs: { 'market:oi_agg': 1_000 },
      staleMultiplier: 1,
      logThrottleMs: 0,
    });

    monitor.start();

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 1_000,
      openInterest: 100,
      openInterestUnit: 'base',
      marketType: 'futures',
      provider: 'local_oi_agg',
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    });

    bus.publish('market:oi_agg', {
      symbol: 'BTCUSDT',
      ts: 7_000,
      openInterest: 100,
      openInterestUnit: 'base',
      marketType: 'futures',
      provider: 'local_oi_agg',
      meta: createMeta('global_data', { tsEvent: 7_000, tsIngest: 7_000 }),
    });

    const warning = lines.find((line) => line.includes('[GlobalData] stale'));
    expect(warning).toBeTruthy();
    expect(warning).toContain('key=market:oi_agg:BTCUSDT:local_oi_agg');
    expect(warning).toContain('sourceId=local_oi_agg:market:oi_agg');

    const uiLine = formatDataSourceDegradedUiLine({
      appTag: '[app]',
      keys: ['market:oi_agg:BTCUSDT:local_oi_agg'],
      sourceId: 'local_oi_agg:market:oi_agg',
      reason: 'stale',
    });

    expect(uiLine).toContain('market:oi_agg:BTCUSDT:local_oi_agg');
    monitor.stop();
  });
});

