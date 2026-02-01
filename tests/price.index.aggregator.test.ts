import { describe, expect, it } from 'vitest';
import { PriceIndexAggregator } from '../src/globalData/PriceIndexAggregator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketPriceIndexEvent, type TickerEvent } from '../src/core/events/EventBus';

describe('PriceIndexAggregator', () => {
  it('aggregates index price with weights and drops stale sources', () => {
    const bus = createTestEventBus();
    const agg = new PriceIndexAggregator(bus, { ttlMs: 1000, weights: { s1: 1, s2: 3 } });
    const outputs: MarketPriceIndexEvent[] = [];
    bus.subscribe('market:price_index', (evt) => outputs.push(evt));
    agg.start();

    const makeTicker = (streamId: string, ts: number, indexPrice: string): TickerEvent => ({
      symbol: 'BTCUSDT',
      streamId,
      marketType: 'futures',
      indexPrice,
      exchangeTs: ts,
      meta: createMeta('market', { tsEvent: ts, tsIngest: ts, streamId }),
    });

    bus.publish('market:ticker', makeTicker('s1', 1000, '100'));
    bus.publish('market:ticker', makeTicker('s2', 1100, '110'));

    const first = outputs[outputs.length - 1];
    expect(first.indexPrice).toBeCloseTo((100 * 1 + 110 * 3) / 4, 8);
    expect(first.venueBreakdown?.s1).toBe(100);
    expect(first.venueBreakdown?.s2).toBe(110);

    bus.publish('market:ticker', makeTicker('s1', 3000, '105'));

    const last = outputs[outputs.length - 1];
    expect(last.indexPrice).toBeCloseTo(105, 8);
    expect(last.sourcesUsed).toEqual(['s1']);
    expect(last.qualityFlags?.staleSourcesDropped).toEqual(['s2']);

    agg.stop();
  });
});
