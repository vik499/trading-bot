import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createMeta,
  type AnalyticsFeaturesEvent,
  type AnalyticsReadyEvent,
  type EventBus,
  type KlineEvent,
  type MarketContextEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { KlineFeatureEngine } from '../src/analytics/FeatureEngine';
import { MarketContextBuilder } from '../src/analytics/MarketContextBuilder';

const makeKline = (symbol: string, index: number, close: number, opts: { correlationId?: string; range?: number } = {}): KlineEvent => {
  const startTs = 1_700_000_000_000 + index * 300_000;
  const endTs = startTs + 300_000;
  const range = opts.range ?? 0.02;
  return {
    symbol,
    streamId: 'bybit.public.linear.v5',
    marketType: 'futures',
    interval: '5',
    tf: '5m',
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
      correlationId: opts.correlationId,
    }),
  };
};

describe('KlineFeatureEngine indicators', () => {
  let bus: EventBus;
  let engine: KlineFeatureEngine;
  let features: AnalyticsFeaturesEvent[];
  let readies: AnalyticsReadyEvent[];

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new KlineFeatureEngine(bus);
    features = [];
    readies = [];
    engine.start();
    bus.subscribe('analytics:features', (evt) => features.push(evt));
    bus.subscribe('analytics:ready', (evt) => readies.push(evt));
  });

  afterEach(() => {
    engine.stop();
  });

  it('computes EMA/RSI/ATR and keeps RSI within bounds', () => {
    for (let i = 0; i < 40; i++) {
      const close = 100 + i * 0.1;
      bus.publish('market:kline', makeKline('BTCUSDT', i, close));
    }

    const klineFeatures = features.filter((f) => f.sourceTopic === 'market:kline');
    expect(klineFeatures.length).toBeGreaterThan(0);

    const filtered = klineFeatures.filter((f) => f.emaFast !== undefined && f.emaSlow !== undefined);
    for (let i = 1; i < filtered.length; i++) {
      const prev = filtered[i - 1];
      const curr = filtered[i];
      expect(curr.emaFast).toBeGreaterThanOrEqual(prev.emaFast as number);
      expect(curr.emaSlow).toBeGreaterThanOrEqual(prev.emaSlow as number);
    }

    const last = klineFeatures[klineFeatures.length - 1];
    expect(last.rsi).toBeGreaterThanOrEqual(0);
    expect(last.rsi).toBeLessThanOrEqual(100);
    expect(last.atr).toBeGreaterThan(0);
  });

  it('emits analytics:ready once with deterministic meta.ts and correlationId', () => {
    const correlationId = 'cid-ready-1';
    for (let i = 0; i < 30; i++) {
      bus.publish('market:kline', makeKline('ETHUSDT', i, 200 + i * 0.05, { correlationId }));
    }

    expect(readies.length).toBe(1);
    const ready = readies[0];
    expect(ready.symbol).toBe('ETHUSDT');
    expect(ready.tf).toBe('5m');
    expect(ready.ready).toBe(true);
    expect(ready.reason).toBe('klineWarmup');
    expect(ready.meta.correlationId).toBe(correlationId);
    expect(ready.meta.ts).toBe(ready.ts);
  });
});

describe('MarketContextBuilder regimes from kline features', () => {
  let bus: EventBus;
  let engine: KlineFeatureEngine;
  let context: MarketContextEvent[];
  let builder: MarketContextBuilder;

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new KlineFeatureEngine(bus);
    builder = new MarketContextBuilder(bus, { macroTfs: ['5m'] });
    context = [];
    engine.start();
    builder.start();
    bus.subscribe('analytics:context', (evt) => context.push(evt));
  });

  afterEach(() => {
    builder.stop();
    engine.stop();
  });

  it('classifies bull and bear trends deterministically', () => {
    for (let i = 0; i < 40; i++) {
      bus.publish('market:kline', makeKline('BULLUSDT', i, 100 + i * 0.1));
    }
    const bull = context.filter((c) => c.symbol === 'BULLUSDT').pop();
    expect(bull?.regimeV2).toBe('trend_bull');
    expect(bull?.regime).toBe('calm');

    for (let i = 0; i < 40; i++) {
      bus.publish('market:kline', makeKline('BEARUSDT', i, 200 - i * 0.1));
    }
    const bear = context.filter((c) => c.symbol === 'BEARUSDT').pop();
    expect(bear?.regimeV2).toBe('trend_bear');
    expect(bear?.regime).toBe('calm');
  });

  it('classifies storm regime on high ATR', () => {
    for (let i = 0; i < 40; i++) {
      bus.publish('market:kline', makeKline('STORMUSDT', i, 100 + i * 0.05, { range: 15 }));
    }
    const storm = context.filter((c) => c.symbol === 'STORMUSDT').pop();
    expect(storm?.regimeV2).toBe('storm');
    expect(storm?.regime).toBe('volatile');
  });
});
