import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { createMeta, type AnalyticsFeaturesEvent, type MarketDataStatusPayload } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { StrategyEngine } from '../src/strategy/StrategyEngine';
import { logger } from '../src/infra/logger';

describe('StrategyEngine market data skip log rate limit', () => {
  const symbol = 'BTCUSDT';
  let engine: StrategyEngine;
  const bus = createTestEventBus();

  beforeEach(() => {
    engine = new StrategyEngine(bus, { marketNotReadyLogMs: 5_000 });
    engine.start();
  });

  afterEach(() => {
    engine.stop();
  });

  it('logs once for repeated skip reasons and logs on change', () => {
    const debugSpy = vi.spyOn(logger, 'debug');

    const status: MarketDataStatusPayload = {
      overallConfidence: 0.1,
      blockConfidence: { price: 0, flow: 0, liquidity: 0, derivatives: 0 },
      degraded: true,
      degradedReasons: ['PRICE_STALE'],
      warmingUp: true,
      warmingProgress: 0,
      warmingWindowMs: 10_000,
      activeSources: 0,
      expectedSources: 1,
      activeSourcesAgg: 0,
      activeSourcesRaw: 0,
      expectedSourcesAgg: 1,
      expectedSourcesRaw: 1,
      lastBucketTs: 1_000,
      meta: createMeta('system', { ts: 1_000 }),
    };
    bus.publish('system:market_data_status', status);

    for (let i = 0; i < 100; i += 1) {
      const ts = 1_000 + i * 10;
      const evt: AnalyticsFeaturesEvent = {
        symbol,
        ts,
        lastPrice: 100,
        sampleCount: 1,
        featuresReady: true,
        windowSize: 10,
        momentum: 0.01,
        meta: createMeta('analytics', { ts }),
      };
      bus.publish('analytics:features', evt);
    }

    let logs = debugSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('market data'));

    expect(logs.length).toBe(1);

    const statusChanged: MarketDataStatusPayload = {
      ...status,
      degradedReasons: ['SOURCES_MISSING'],
      meta: createMeta('system', { ts: 2_000 }),
    };
    bus.publish('system:market_data_status', statusChanged);

    const evtAfterChange: AnalyticsFeaturesEvent = {
      symbol,
      ts: 2_000,
      lastPrice: 100,
      sampleCount: 1,
      featuresReady: true,
      windowSize: 10,
      momentum: 0.01,
      meta: createMeta('analytics', { ts: 2_000 }),
    };
    bus.publish('analytics:features', evtAfterChange);

    logs = debugSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('market data'));

    expect(logs.length).toBe(2);

    debugSpy.mockRestore();
  });

  it('logs suppressedCount after cooldown', () => {
    const debugSpy = vi.spyOn(logger, 'debug');

    const status: MarketDataStatusPayload = {
      overallConfidence: 0.1,
      blockConfidence: { price: 0, flow: 0, liquidity: 0, derivatives: 0 },
      degraded: true,
      degradedReasons: ['PRICE_STALE'],
      warmingUp: false,
      warmingProgress: 0,
      warmingWindowMs: 10_000,
      activeSources: 0,
      expectedSources: 1,
      activeSourcesAgg: 0,
      activeSourcesRaw: 0,
      expectedSourcesAgg: 1,
      expectedSourcesRaw: 1,
      lastBucketTs: 1_000,
      meta: createMeta('system', { ts: 1_000 }),
    };
    bus.publish('system:market_data_status', status);

    for (let i = 0; i < 10; i += 1) {
      const ts = 1_000 + i * 100;
      const evt: AnalyticsFeaturesEvent = {
        symbol,
        ts,
        lastPrice: 100,
        sampleCount: 1,
        featuresReady: true,
        windowSize: 10,
        momentum: 0.01,
        meta: createMeta('analytics', { ts }),
      };
      bus.publish('analytics:features', evt);
    }

    let logs = debugSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('market data'));

    expect(logs.length).toBe(1);

    const evtAfterCooldown: AnalyticsFeaturesEvent = {
      symbol,
      ts: 7_000,
      lastPrice: 100,
      sampleCount: 1,
      featuresReady: true,
      windowSize: 10,
      momentum: 0.01,
      meta: createMeta('analytics', { ts: 7_000 }),
    };
    bus.publish('analytics:features', evtAfterCooldown);

    logs = debugSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('market data'));

    expect(logs.length).toBe(2);
    expect(logs[1]).toContain('suppressedCount=9');

    debugSpy.mockRestore();
  });
});
