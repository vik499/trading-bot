import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createMeta, type AnalyticsFeaturesEvent, type MarketContextEvent, type EventBus } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { StrategyEngine } from '../src/strategy/StrategyEngine';
import { logger } from '../src/infra/logger';

describe.sequential('StrategyEngine decision trace', () => {
  const symbol = 'BTCUSDT';
  let engine: StrategyEngine;
  let bus: EventBus;

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new StrategyEngine(bus, { decisionTraceIntervalMs: 1_000, minIntentIntervalMs: 10_000 });
    engine.start();
  });

  afterEach(() => {
    engine.stop();
  });

  it('logs reject summary when gating blocks intent', () => {
    const corrId = 'trace-ctx-missing';
    const infoSpy = vi.spyOn(logger, 'info');
    const intents: unknown[] = [];
    bus.subscribe('strategy:intent', (evt) => intents.push(evt));

    const evt: AnalyticsFeaturesEvent = {
      symbol,
      ts: 1_700_000_000_000,
      lastPrice: 100,
      sampleCount: 1,
      featuresReady: true,
      windowSize: 10,
      momentum: 0.01,
      meta: createMeta('analytics', { ts: 1_700_000_000_000, correlationId: corrId }),
    };

    bus.publish('analytics:features', evt);

    const logs = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes(corrId));

    expect(logs.length).toBe(1);
    expect(logs[0]).toContain('[Strategy] reject summary');
    expect(logs[0]).toContain('contextMissing');
    expect(intents).toHaveLength(0);

    infoSpy.mockRestore();
  });

  it('throttles reject summary by event.meta.ts', () => {
    const corrId = 'trace-throttle';
    const infoSpy = vi.spyOn(logger, 'info');

    for (let i = 0; i < 100; i += 1) {
      const ts = 1_700_000_000_000 + i * 10;
      const evt: AnalyticsFeaturesEvent = {
        symbol,
        ts,
        lastPrice: 100,
        sampleCount: 1,
        featuresReady: true,
        windowSize: 10,
        momentum: 0.01,
        meta: createMeta('analytics', { ts, correlationId: corrId }),
      };
      bus.publish('analytics:features', evt);
    }

    const logs = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes(corrId));

    expect(logs.length).toBe(1);

    infoSpy.mockRestore();
  });

  it('logs on reason change even within throttle interval', () => {
    const corrId = 'trace-change';
    const infoSpy = vi.spyOn(logger, 'info');

    const context: MarketContextEvent = {
      symbol,
      ts: 1_700_000_010_000,
      regime: 'calm',
      featuresReady: true,
      contextReady: false,
      processedCount: 1,
      meta: createMeta('analytics', { ts: 1_700_000_010_000, correlationId: corrId }),
    };
    bus.publish('analytics:context', context);

    const firstEvt: AnalyticsFeaturesEvent = {
      symbol,
      ts: 1_700_000_010_000,
      lastPrice: 100,
      sampleCount: 1,
      featuresReady: true,
      windowSize: 10,
      momentum: 0.01,
      meta: createMeta('analytics', { ts: 1_700_000_010_000, correlationId: corrId }),
    };
    bus.publish('analytics:features', firstEvt);

    const readyContext: MarketContextEvent = {
      ...context,
      ts: 1_700_000_010_100,
      contextReady: true,
      meta: createMeta('analytics', { ts: 1_700_000_010_100, correlationId: corrId }),
    };
    bus.publish('analytics:context', readyContext);

    const secondEvt: AnalyticsFeaturesEvent = {
      ...firstEvt,
      ts: 1_700_000_010_100,
      meta: createMeta('analytics', { ts: 1_700_000_010_100, correlationId: corrId }),
      momentum: 0.0,
    };
    bus.publish('analytics:features', secondEvt);

    const logs = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes(corrId));

    expect(logs.length).toBe(2);
    expect(logs[0]).toContain('contextNotReady');
    expect(logs[1]).toContain('momentumBelowThreshold');

    infoSpy.mockRestore();
  });
});
