import { describe, expect, it } from 'vitest';
import { createMeta, type MarketPriceCanonicalEvent, type TickerEvent } from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { CanonicalPriceAggregator } from '../../src/globalData/CanonicalPriceAggregator';

describe('CanonicalPriceAggregator', () => {
  it('never emits price_canonical without sourcesUsed', () => {
    const bus = createTestEventBus();
    const aggregator = new CanonicalPriceAggregator(bus, { ttlMs: 60_000 });
    const emitted: MarketPriceCanonicalEvent[] = [];

    bus.subscribe('market:price_canonical', (evt) => emitted.push(evt));
    aggregator.start();

    const noPriceTicker: TickerEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      marketType: 'futures',
      meta: createMeta('market', { ts: 1000 }),
    };
    bus.publish('market:ticker', noPriceTicker);

    expect(emitted).toHaveLength(0);

    const pricedTicker: TickerEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      marketType: 'futures',
      indexPrice: '100',
      meta: createMeta('market', { ts: 2000 }),
    };
    bus.publish('market:ticker', pricedTicker);

    expect(emitted).toHaveLength(1);
    expect(emitted[0].sourcesUsed.length).toBeGreaterThan(0);

    aggregator.stop();
  });
});
