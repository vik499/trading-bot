import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { FeatureEngine } from '../src/analytics/FeatureEngine';
import { MarketContextBuilder } from '../src/analytics/MarketContextBuilder';
import { createMeta, type AnalyticsFeaturesEvent, type AnalyticsReadyEvent, type EventBus, type MarketContextEvent, type TickerEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';

const makeTicker = (symbol: string, ts: number, price: number): TickerEvent => ({
  symbol,
  streamId: 'test.stream',
  marketType: 'futures',
  lastPrice: String(price),
  meta: createMeta('market', { tsEvent: ts, tsIngest: ts }),
});

describe('FeatureEngine analytics:features', () => {
  let bus: EventBus;
  let engine: FeatureEngine;
  let emitted: AnalyticsFeaturesEvent[];
  let unsubscribe: (() => void) | undefined;

  beforeEach(() => {
    bus = createTestEventBus();
    emitted = [];
    engine = new FeatureEngine(bus, { windowSize: 25, smaPeriod: 20, minEmitIntervalMs: 0, maxTicksBeforeEmit: 1 });
    engine.start();
    const handler = (evt: AnalyticsFeaturesEvent) => emitted.push(evt);
    bus.subscribe('analytics:features', handler);
    unsubscribe = () => bus.unsubscribe('analytics:features', handler);
  });

  afterEach(() => {
    unsubscribe?.();
    engine.stop();
  });

  it('emits readiness once sma window filled and preserves meta.ts', () => {
    for (let i = 1; i <= 22; i++) {
      const ts = 1000 + i * 100;
      bus.publish('market:ticker', makeTicker('BTCUSDT', ts, 100 + i));
    }

    const beforeReady = emitted.find((e) => e.sampleCount === 10);
    const ready = emitted.find((e) => e.sampleCount === 20);

    expect(beforeReady?.featuresReady).toBe(false);
    expect(beforeReady?.sma20).toBeUndefined();
    expect(ready?.featuresReady).toBe(true);
    expect(typeof ready?.sma20).toBe('number');
    expect(ready?.meta.ts).toBe(ready?.ts);
  });
});

describe('MarketContextBuilder analytics:context', () => {
  let bus: EventBus;
  let engine: FeatureEngine;
  let builder: MarketContextBuilder;
  let contexts: MarketContextEvent[];
  let readySignals: AnalyticsReadyEvent[];
  let unsubscribe: (() => void) | undefined;
  let unsubscribeReady: (() => void) | undefined;

  beforeEach(() => {
    bus = createTestEventBus();
    contexts = [];
    readySignals = [];
    engine = new FeatureEngine(bus, { smaPeriod: 3, windowSize: 5, minEmitIntervalMs: 0, maxTicksBeforeEmit: 1 });
    builder = new MarketContextBuilder(bus, { contextWarmup: 2, lowVolThreshold: 0.0005, highVolThreshold: 0.0015 });
    engine.start();
    builder.start();
    const handler = (evt: MarketContextEvent) => contexts.push(evt);
    bus.subscribe('analytics:context', handler);
    unsubscribe = () => bus.unsubscribe('analytics:context', handler);
    const readyHandler = (evt: AnalyticsReadyEvent) => readySignals.push(evt);
    bus.subscribe('analytics:ready', readyHandler);
    unsubscribeReady = () => bus.unsubscribe('analytics:ready', readyHandler);
  });

  afterEach(() => {
    unsubscribe?.();
    unsubscribeReady?.();
    builder.stop();
    engine.stop();
  });

  it('produces calm then volatile regimes based on volatility thresholds', () => {
    const baseTs = 1_000;
    const pricesLow = [100, 100.01, 100.02, 100.03];
    pricesLow.forEach((p, idx) => bus.publish('market:ticker', makeTicker('ETHUSDT', baseTs + idx * 10, p)));

    const lastCalm = contexts[contexts.length - 1];
    expect(lastCalm?.regime).toBe('calm');
    expect(lastCalm?.contextReady).toBe(true);

    const pricesHigh = [100.03, 100.5, 99.9, 101];
    pricesHigh.forEach((p, idx) => bus.publish('market:ticker', makeTicker('ETHUSDT', baseTs + 100 + idx * 10, p)));

    const final = contexts[contexts.length - 1];
    expect(final?.regime).toBe('volatile');
    expect(final?.contextReady).toBe(true);
    expect(final?.meta.ts).toBe(final?.ts);

    expect(readySignals.length).toBeGreaterThanOrEqual(1);
    expect(readySignals[0]?.symbol).toBe('ETHUSDT');
    expect(readySignals[0]?.meta.ts).toBe(readySignals[0]?.ts);
    expect(readySignals[0]?.ready).toBe(true);
    expect(readySignals[0]?.reason).toBe('tickerWarmup');
  });
});

describe('FeatureEngine throttling', () => {
  let bus: EventBus;
  let engine: FeatureEngine;
  let emitted: AnalyticsFeaturesEvent[];
  let unsubscribe: (() => void) | undefined;

  beforeEach(() => {
    bus = createTestEventBus();
    emitted = [];
    engine = new FeatureEngine(bus); // defaults: 1s interval, emit every 5 ticks max
    engine.start();
    const handler = (evt: AnalyticsFeaturesEvent) => emitted.push(evt);
    bus.subscribe('analytics:features', handler);
    unsubscribe = () => bus.unsubscribe('analytics:features', handler);
  });

  afterEach(() => {
    unsubscribe?.();
    engine.stop();
  });

  it('limits emission cadence using interval and tick gate', () => {
    const symbol = 'SOLUSDT';
    for (let i = 0; i < 12; i++) {
      const ts = 1_000 + i * 100;
      bus.publish('market:ticker', makeTicker(symbol, ts, 10 + i));
    }

    expect(emitted.length).toBe(3);
    expect(emitted.map((e) => e.ts)).toEqual([1_000, 1_500, 2_000]);
  });
});
