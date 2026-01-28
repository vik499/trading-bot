import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';
import { createMeta, type MarketDataStatusPayload, type MarketPriceCanonicalEvent, type TradeEvent } from '../src/core/events/EventBus';
import { SourceRegistry } from '../src/core/market/SourceRegistry';

describe('MarketDataReadiness no-data watchdog', () => {
  it('marks readiness degraded when raw feeds stall beyond noDataWindowMs', () => {
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1_000,
      warmingWindowMs: 1_000,
      expectedSources: 0,
      noDataWindowMs: 500,
      logIntervalMs: 0,
      sourceRegistry: registry,
      readinessStabilityWindowMs: 0,
    });

    const statuses: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    readiness.start();

    const trade: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 'test-stream',
      side: 'Buy',
      price: 100,
      size: 1,
      tradeTs: 1_000,
      exchangeTs: 1_000,
      meta: createMeta('market', { ts: 1_000 }),
    };
    bus.publish('market:trade', trade);

    const price: MarketPriceCanonicalEvent = {
      symbol: 'BTCUSDT',
      ts: 3_000,
      lastPrice: 101,
      sourcesUsed: ['test-stream'],
      freshSourcesCount: 1,
      confidenceScore: 1,
      meta: createMeta('global_data', { ts: 3_000 }),
    };
    bus.publish('market:price_canonical', price);

    const last = statuses[statuses.length - 1];
    expect(last).toBeTruthy();
    expect(last?.degradedReasons).toContain('NO_DATA');
  });
});
