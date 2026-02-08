import { describe, expect, it } from 'vitest';
import { createMeta, type MarketDataStatusPayload } from '../../src/core/events/EventBus';
import { buildMarketReadinessSnapshot } from '../../src/observability/MarketDataReadiness';

describe('MarketDataReadiness snapshot', () => {
  it('preserves degraded reasons and warnings in health snapshot', () => {
    const payload: MarketDataStatusPayload = {
      overallConfidence: 0.4,
      blockConfidence: {
        price: 0.4,
        flow: 0.4,
        liquidity: 0.4,
        derivatives: 0.4,
      },
      degraded: true,
      degradedReasons: ['PRICE_STALE', 'NO_DATA'],
      warnings: ['EXCHANGE_LAG_TOO_HIGH'],
      warmingUp: false,
      warmingProgress: 1,
      warmingWindowMs: 60_000,
      activeSources: 1,
      expectedSources: 1,
      activeSourcesAgg: 1,
      activeSourcesRaw: 1,
      expectedSourcesAgg: 1,
      expectedSourcesRaw: 1,
      lastBucketTs: 1_000,
      meta: createMeta('global_data', { tsEvent: 1_000, tsIngest: 1_000 }),
    };

    const snapshot = buildMarketReadinessSnapshot(payload, 'BTCUSDT', 'futures');

    expect(snapshot.status).toBe('DEGRADED');
    expect(snapshot.degradedReasons).toEqual(['PRICE_STALE', 'NO_DATA']);
    expect(snapshot.warnings).toEqual(['EXCHANGE_LAG_TOO_HIGH']);
  });
});
