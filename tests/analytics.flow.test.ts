import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { FlowEngine } from '../src/analytics/FlowEngine';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type AnalyticsFlowEvent, type FundingRateEvent, type OpenInterestEvent, type TradeEvent } from '../src/core/events/EventBus';

describe('FlowEngine', () => {
  let bus = createTestEventBus();
  let engine: FlowEngine;

  beforeEach(() => {
    bus = createTestEventBus();
    engine = new FlowEngine(bus, { minEmitIntervalMs: 0, maxTradesBeforeEmit: 1 });
    engine.start();
  });

  afterEach(() => {
    engine.stop();
  });

  it('emits flow updates with inherited meta and cvd/oi/funding', () => {
    const events: AnalyticsFlowEvent[] = [];
    bus.subscribe('analytics:flow', (evt) => events.push(evt));

    const trade: TradeEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      tradeId: 't-1',
      side: 'Buy',
      price: 100,
      size: 2,
      tradeTs: 900,
      exchangeTs: 900,
      meta: createMeta('market', { ts: 1000, correlationId: 'corr-1' }),
    };
    bus.publish('market:trade', trade);

    const oi: OpenInterestEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      openInterest: 1000,
      openInterestUnit: 'base',
      exchangeTs: 1500,
      meta: createMeta('market', { ts: 2000, correlationId: 'corr-1' }),
    };
    bus.publish('market:oi', oi);

    const funding: FundingRateEvent = {
      symbol: 'BTCUSDT',
      streamId: 's1',
      fundingRate: 0.0001,
      exchangeTs: 2500,
      meta: createMeta('market', { ts: 3000, correlationId: 'corr-1' }),
    };
    bus.publish('market:funding', funding);

    expect(events.length).toBeGreaterThanOrEqual(3);
    const tradeFlow = events[0];
    expect(tradeFlow.meta.correlationId).toBe('corr-1');
    expect(tradeFlow.meta.ts).toBe(1000);
    expect(tradeFlow.cvdFutures).toBe(2);
    expect(tradeFlow.flowRegime).toBe('buyPressure');

    const oiFlow = events.find((evt) => evt.oi !== undefined);
    expect(oiFlow?.oi).toBe(1000);
    expect(oiFlow?.meta.ts).toBe(2000);

    const fundingFlow = events.find((evt) => evt.fundingRate !== undefined);
    expect(fundingFlow?.fundingRate).toBe(0.0001);
    expect(fundingFlow?.meta.ts).toBe(3000);
  });
});
