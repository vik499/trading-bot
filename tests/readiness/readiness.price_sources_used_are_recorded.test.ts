import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type MarketDataStatusPayload,
  type MarketPriceCanonicalEvent,
} from '../../src/core/events/EventBus';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const makePrice = (ts: number): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  indexPrice: 100,
  sourcesUsed: ['binance.spot.public'],
  freshSourcesCount: 1,
  confidenceScore: 0.9,
  meta: createMeta('global_data', { ts }),
});

describe('MarketDataReadiness price sourcesUsed recording', () => {
  it('records usedAgg.price and lastSeenAggTs', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      expectedSourcesByBlock: { price: ['binance.spot.public'] },
    });
    const outputs: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => outputs.push(evt));
    readiness.start();

    bus.publish('market:price_canonical', makePrice(1000));

    const snapshot = (readiness as any).buildSnapshot(1000);
    expect(snapshot.usedAgg.price).toEqual(['binance.spot.public']);
    expect(snapshot.lastSeenAggTs.priceCanonical).toBe(1000);

    const last = outputs[outputs.length - 1];
    expect(last.blockConfidence.price).toBeGreaterThan(0);

    readiness.stop();
  });
});
