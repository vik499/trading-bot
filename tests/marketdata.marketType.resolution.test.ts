import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { SourceRegistry } from '../src/core/market/SourceRegistry';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';
import { createMeta, type OrderbookL2SnapshotEvent } from '../src/core/events/EventBus';

describe('MarketDataReadiness marketType resolution', () => {
  it('tracks orderbook under explicit marketType', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1_000,
      warmingWindowMs: 0,
    });
    readiness.start();

    const evt: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      updateId: 1,
      exchangeTs: 1_000,
      bids: [],
      asks: [],
      marketType: 'futures',
      meta: createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId: 'binance.usdm.public' }),
    };

    bus.publish('market:orderbook_l2_snapshot', evt);

    const snapshot = registry.snapshot(1_000, 'BTCUSDT', 'futures');
    expect(snapshot.usedRaw.orderbook).toContain('binance.usdm.public');

    readiness.stop();
  });

  it('infers marketType from streamId when missing', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1_000,
      warmingWindowMs: 0,
    });
    readiness.start();

    const evt: OrderbookL2SnapshotEvent = {
      symbol: 'BTCUSDT',
      streamId: 'binance.spot.public',
      updateId: 2,
      exchangeTs: 2_000,
      bids: [],
      asks: [],
      marketType: 'spot',
      meta: createMeta('market', { tsEvent: 2_000, tsIngest: 2_000, streamId: 'binance.spot.public' }),
    };

    bus.publish('market:orderbook_l2_snapshot', evt);

    const snapshot = registry.snapshot(2_000, 'BTCUSDT', 'spot');
    expect(snapshot.usedRaw.orderbook).toContain('binance.spot.public');

    readiness.stop();
  });
});
