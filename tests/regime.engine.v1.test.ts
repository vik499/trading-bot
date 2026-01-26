import { describe, it, expect } from 'vitest';
import { createMeta, type AnalyticsMarketViewEvent } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { RegimeEngineV1 } from '../src/analytics/RegimeEngineV1';

describe('RegimeEngineV1', () => {
  it('emits trend and storm regimes deterministically', () => {
    const bus = createTestEventBus();
    const engine = new RegimeEngineV1(bus, { explainThrottleMs: 0, highVolThreshold: 0.01 });
    engine.start();

    const regimes: string[] = [];
    bus.subscribe('analytics:regime', (evt) => regimes.push(`${evt.regime}:${evt.regimeV2}:${evt.meta.ts}`));

    const seed: AnalyticsMarketViewEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      macro: { tf: '1h', close: 100, emaFast: 101, emaSlow: 100, atr: 0.2 },
      meta: createMeta('analytics', { ts: 1000 }),
    };
    bus.publish('analytics:market_view', seed);

    const bull: AnalyticsMarketViewEvent = {
      symbol: 'BTCUSDT',
      ts: 2000,
      macro: { tf: '1h', close: 102, emaFast: 105, emaSlow: 103, atr: 0.2 },
      meta: createMeta('analytics', { ts: 2000 }),
    };
    bus.publish('analytics:market_view', bull);

    const storm: AnalyticsMarketViewEvent = {
      symbol: 'BTCUSDT',
      ts: 3000,
      macro: { tf: '1h', close: 100, emaFast: 101, emaSlow: 102, atr: 5 },
      meta: createMeta('analytics', { ts: 3000 }),
    };
    bus.publish('analytics:market_view', storm);

    expect(regimes[1]).toBe('calm:trend_bull:2000');
    expect(regimes[2]).toBe('volatile:storm:3000');

    engine.stop();
  });
});
