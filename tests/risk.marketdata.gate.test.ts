import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketDataStatusPayload, type RiskRejectedIntentEvent, type StrategyIntentEvent } from '../src/core/events/EventBus';
import { RiskManager } from '../src/risk/RiskManager';

describe('RiskManager market data gate', () => {
  it('rejects intents when market data is warming or degraded', () => {
    const bus = createTestEventBus();
    const risk = new RiskManager(bus, { cooldownMs: 0 });
    const rejected: RiskRejectedIntentEvent[] = [];

    bus.subscribe('risk:rejected_intent', (evt) => rejected.push(evt));
    risk.start();

    const status: MarketDataStatusPayload = {
      meta: createMeta('system', { ts: 1000 }),
      overallConfidence: 0,
      blockConfidence: { price: 0, flow: 0, liquidity: 0, derivatives: 0 },
      degraded: true,
      degradedReasons: ['WS_DISCONNECTED'],
      warmingUp: true,
      warmingProgress: 0,
      warmingWindowMs: 1000,
      activeSources: 0,
      expectedSources: 0,
      activeSourcesAgg: 0,
      activeSourcesRaw: 0,
      expectedSourcesAgg: 0,
      expectedSourcesRaw: 0,
      lastBucketTs: 1000,
    };

    bus.publish('system:market_data_status', status);

    const intent: StrategyIntentEvent = {
      intentId: 'intent-1',
      symbol: 'BTCUSDT',
      side: 'LONG',
      targetExposureUsd: 10,
      reason: 'test',
      ts: 1000,
      meta: createMeta('strategy', { ts: 1000 }),
    };

    bus.publish('strategy:intent', intent);

    expect(rejected).toHaveLength(1);
    expect(rejected[0].reasonCode).toBe('MARKET_DATA');

    risk.stop();
  });
});
