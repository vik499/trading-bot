import { describe, expect, it } from 'vitest';
import { CanonicalPriceAggregator } from '../src/globalData/CanonicalPriceAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketPriceCanonicalEvent, type TickerEvent } from '../src/core/events/EventBus';

const makeTicker = (ts: number, overrides: Partial<TickerEvent> = {}): TickerEvent => ({
  symbol: 'BTCUSDT',
  streamId: 's1',
  indexPrice: '100',
  markPrice: '99.5',
  lastPrice: '100.2',
  exchangeTs: ts,
  meta: createMeta('market', { ts }),
  ...overrides,
});

describe('CanonicalPriceAggregator fallback', () => {
  it('emits sourcesUsed for canonical price', () => {
    const bus = createTestEventBus();
    const agg = new CanonicalPriceAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketPriceCanonicalEvent[] = [];
    bus.subscribe('market:price_canonical', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:ticker', makeTicker(1_000));

    const last = outputs[outputs.length - 1];
    expect(last.sourcesUsed?.length ?? 0).toBeGreaterThan(0);

    agg.stop();
  });
  it('uses index when available', () => {
    const bus = createTestEventBus();
    const agg = new CanonicalPriceAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketPriceCanonicalEvent[] = [];
    bus.subscribe('market:price_canonical', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:ticker', makeTicker(1_000));

    const last = outputs[outputs.length - 1];
    expect(last.priceTypeUsed).toBe('index');
    expect(last.fallbackReason).toBeUndefined();

    agg.stop();
  });

  it('falls back to mark when index is stale', () => {
    const bus = createTestEventBus();
    const agg = new CanonicalPriceAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketPriceCanonicalEvent[] = [];
    bus.subscribe('market:price_canonical', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:ticker', makeTicker(0, { indexPrice: '100', markPrice: undefined, lastPrice: undefined }));
    bus.publish('market:ticker', makeTicker(2_000, { indexPrice: undefined, markPrice: '99.9' }));

    const last = outputs[outputs.length - 1];
    expect(last.priceTypeUsed).toBe('mark');
    expect(last.fallbackReason).toBe('INDEX_STALE');

    agg.stop();
  });

  it('falls back to mark when index missing', () => {
    const bus = createTestEventBus();
    const agg = new CanonicalPriceAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketPriceCanonicalEvent[] = [];
    bus.subscribe('market:price_canonical', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:ticker', makeTicker(1_000, { indexPrice: undefined, markPrice: '99.7' }));

    const last = outputs[outputs.length - 1];
    expect(last.priceTypeUsed).toBe('mark');
    expect(last.fallbackReason).toBe('NO_INDEX');

    agg.stop();
  });

  it('falls back to last when mark is stale', () => {
    const bus = createTestEventBus();
    const agg = new CanonicalPriceAggregator(bus, { ttlMs: 1_000 });
    const outputs: MarketPriceCanonicalEvent[] = [];
    bus.subscribe('market:price_canonical', (evt) => outputs.push(evt));
    agg.start();

    bus.publish('market:ticker', makeTicker(0, { indexPrice: undefined, markPrice: '99.9', lastPrice: undefined }));
    bus.publish('market:ticker', makeTicker(2_000, { indexPrice: undefined, markPrice: undefined, lastPrice: '100.1' }));

    const last = outputs[outputs.length - 1];
    expect(last.priceTypeUsed).toBe('last');
    expect(last.fallbackReason).toBe('MARK_STALE');
    expect(last.confidenceScore).toBeLessThan(1);

    agg.stop();
  });
});
