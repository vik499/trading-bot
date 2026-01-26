import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness seedExpectedSources', () => {
  it('pre-registers expected sources before agg events', () => {
    const bus = createTestEventBus();
    const readiness = new MarketDataReadiness(bus, {
      bucketMs: 1000,
      warmingWindowMs: 0,
      logIntervalMs: 0,
      targetMarketType: 'futures',
      expectedSourcesConfig: {
        default: {
          futures: {
            price: ['bybit.public.linear.v5', 'binance.usdm.public'],
            flow: ['bybit.public.linear.v5'],
            liquidity: ['bybit.public.linear.v5'],
            derivatives: ['bybit.public.linear.v5'],
          },
        },
      },
    });

    readiness.seedExpectedSources({ symbol: 'BTCUSDT', marketType: 'futures' });

    const snapshot = (readiness as any).buildSnapshot(1000);
    expect(snapshot.marketType).toBe('futures');
    expect(snapshot.expected.price).toEqual(['bybit.public.linear.v5', 'binance.usdm.public']);
    expect(snapshot.expected.flow).toEqual(['bybit.public.linear.v5']);
    expect(snapshot.expected.liquidity).toEqual(['bybit.public.linear.v5']);
    expect(snapshot.expected.derivatives).toEqual(['bybit.public.linear.v5']);

    readiness.stop();
  });
});
