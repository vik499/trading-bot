import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketPriceCanonicalEvent, type MarketDisconnected, type MarketDataStatusPayload } from '../src/core/events/EventBus';
import { MarketDataReadiness } from '../src/observability/MarketDataReadiness';

const buildPrice = (ts: number): MarketPriceCanonicalEvent => ({
  symbol: 'BTCUSDT',
  ts,
  sourcesUsed: ['bybit.public.linear.v5'],
  freshSourcesCount: 1,
  meta: createMeta('global_data', { ts }),
});

describe('MarketDataReadiness WS degrade/recover', () => {
  it('marks WS disconnected as degraded and recovers after stable flow window', () => {
    const bus = createTestEventBus();
    const statuses: MarketDataStatusPayload[] = [];
    bus.subscribe('system:market_data_status', (evt) => statuses.push(evt));

    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1_000,
      warmingWindowMs: 1_000,
      expectedSources: 0,
      logIntervalMs: 0,
      wsRecoveryWindowMs: 2_000,
    });
    readiness.start();

    const disconnect: MarketDisconnected = {
      meta: createMeta('market', { ts: 1_000 }),
      reason: 'test disconnect',
    };
    bus.publish('market:disconnected', disconnect);

    bus.publish('market:price_canonical', buildPrice(1_000));

    const first = statuses[statuses.length - 1];
    expect(first?.degradedReasons).toContain('WS_DISCONNECTED');

    bus.publish('market:trade', {
      symbol: 'BTCUSDT',
      streamId: 'bybit.public.linear.v5',
      side: 'Buy',
      price: 100,
      size: 1,
      tradeTs: 1_500,
      meta: createMeta('market', { ts: 1_500 }),
    });

    bus.publish('market:price_canonical', buildPrice(3_500));

    const recovered = statuses[statuses.length - 1];
    expect(recovered?.degradedReasons).not.toContain('WS_DISCONNECTED');

    readiness.stop();
  });
});
