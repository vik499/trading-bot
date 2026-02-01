import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createMeta,
  type AnalyticsReadyEvent,
  type EventBus,
  type KlineEvent,
  type MarketContextEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { KlineFeatureEngine } from '../src/analytics/FeatureEngine';
import { MarketContextBuilder } from '../src/analytics/MarketContextBuilder';

const TF_MS: Record<string, number> = {
  '5m': 300_000,
  '1h': 3_600_000,
  '4h': 14_400_000,
  '1d': 86_400_000,
};

const TF_INTERVAL: Record<string, KlineEvent['interval']> = {
  '5m': '5',
  '1h': '60',
  '4h': '240',
  '1d': '1440',
};

const makeKline = (symbol: string, tf: string, index: number, close: number, range = 0.02, correlationId?: string): KlineEvent => {
  const step = TF_MS[tf] ?? 300_000;
  const startTs = 1_700_000_000_000 + index * step;
  const endTs = startTs + step;
  return {
    symbol,
    streamId: 'bybit.public.linear.v5',
    marketType: 'futures',
    interval: TF_INTERVAL[tf] ?? '5',
    tf,
    startTs,
    endTs,
    open: close - range / 2,
    high: close + range,
    low: close - range,
    close,
    volume: 1,
    meta: createMeta('market', {
      tsEvent: endTs,
      tsIngest: endTs,
      streamId: 'bybit.public.linear.v5',
      correlationId,
    }),
  };
};

const publishSeries = (bus: EventBus, symbol: string, tf: string, count: number, base: number, step: number, range: number, cid: string) => {
  for (let i = 0; i < count; i++) {
    const close = base + i * step;
    bus.publish('market:kline', makeKline(symbol, tf, i, close, range, cid));
  }
};

describe('MarketContextBuilder MTF readiness and regime', () => {
  let bus: EventBus;
  let engine: KlineFeatureEngine;
  let builder: MarketContextBuilder;
  let readyEvents: AnalyticsReadyEvent[];
  let contextEvents: MarketContextEvent[];

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new KlineFeatureEngine(bus);
    builder = new MarketContextBuilder(bus);
    readyEvents = [];
    contextEvents = [];
    engine.start();
    builder.start();
    bus.subscribe('analytics:ready', (evt) => readyEvents.push(evt));
    bus.subscribe('analytics:context', (evt) => contextEvents.push(evt));
  });

  afterEach(() => {
    builder.stop();
    engine.stop();
  });

  it('does not emit macro ready when only 5m is warmed', () => {
    const cid = 'macro-miss';
    publishSeries(bus, 'BTCUSDT', '5m', 30, 100, 0.1, 0.05, cid);
    const macro = readyEvents.filter((evt) => evt.reason === 'macroWarmup');
    expect(macro.length).toBe(0);
  });

  it('emits macro ready once when 1h+4h+1d are warmed', () => {
    const cid = 'macro-ok';
    publishSeries(bus, 'ETHUSDT', '1h', 30, 100, 0.1, 0.05, cid);
    publishSeries(bus, 'ETHUSDT', '4h', 30, 200, 0.2, 0.05, cid);
    publishSeries(bus, 'ETHUSDT', '1d', 30, 300, 0.3, 0.05, cid);

    const macro = readyEvents.filter((evt) => evt.reason === 'macroWarmup');
    expect(macro.length).toBe(1);
    expect(macro[0].symbol).toBe('ETHUSDT');
    expect(macro[0].readyTfs?.sort()).toEqual(['1d', '1h', '4h']);
    expect(macro[0].meta.correlationId).toBe(cid);
  });

  it('derives bull trend and storm regimes deterministically', () => {
    const cid = 'macro-regime';
    publishSeries(bus, 'BULLMACRO', '1h', 30, 100, 0.02, 0.005, cid);
    publishSeries(bus, 'BULLMACRO', '4h', 30, 200, 0.02, 0.005, cid);
    publishSeries(bus, 'BULLMACRO', '1d', 30, 300, 0.02, 0.005, cid);

    const bull = contextEvents.filter((evt) => evt.symbol === 'BULLMACRO').pop();
    expect(bull?.regimeV2).toBe('trend_bull');
    expect(bull?.regime).toBe('calm');

    contextEvents = [];
    publishSeries(bus, 'STORMMACRO', '1h', 30, 100, 0.1, 20, cid);
    publishSeries(bus, 'STORMMACRO', '4h', 30, 200, 0.1, 20, cid);
    publishSeries(bus, 'STORMMACRO', '1d', 30, 300, 0.1, 20, cid);

    const storm = contextEvents.filter((evt) => evt.symbol === 'STORMMACRO').pop();
    expect(storm?.regimeV2).toBe('storm');
    expect(storm?.regime).toBe('volatile');
  });
});
