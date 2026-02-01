import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createMeta,
  inheritMeta,
  type AnalyticsFeaturesEvent,
  type BotEventMap,
  type RiskApprovedIntentEvent,
  type StrategyIntentEvent,
  type TickerEvent,
  EventBus,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { InMemoryFeatureStore } from '../src/core/research/FeatureStore';

function waitForEvent<T extends keyof BotEventMap>(bus: EventBus, topic: T, timeoutMs = 1500): Promise<BotEventMap[T][0]> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      bus.unsubscribe(topic, handler as any);
      reject(new Error(`Timed out waiting for event ${topic}`));
    }, timeoutMs);

    const handler = (payload: BotEventMap[T][0]) => {
      clearTimeout(timer);
      bus.unsubscribe(topic, handler as any);
      resolve(payload);
    };

    bus.subscribe(topic, handler as any);
  });
}

describe('Production-like pipeline market -> analytics -> strategy -> risk', () => {
  let bus: EventBus;
  let store: InMemoryFeatureStore;
  let featureHandler: ((t: TickerEvent) => void) | undefined;
  let strategyHandler: ((f: AnalyticsFeaturesEvent) => void) | undefined;
  let riskHandler: ((i: StrategyIntentEvent) => void) | undefined;

  beforeEach(() => {
    bus = createTestEventBus();
    store = new InMemoryFeatureStore(100, bus);
    store.start();

    // FeatureEngine stub
    featureHandler = (ticker: TickerEvent) => {
      const price = Number(ticker.lastPrice ?? 0);
      const features: AnalyticsFeaturesEvent = {
        symbol: ticker.symbol,
        ts: ticker.meta.ts,
        lastPrice: price,
        sma20: price,
        volatility: 0,
        momentum: 0,
        return1: 0,
        sampleCount: 1,
        featuresReady: true,
        windowSize: 1,
        smaPeriod: 1,
        meta: inheritMeta(ticker.meta, 'analytics', { ts: ticker.meta.ts }),
      };
      bus.publish('analytics:features', features);
    };
    bus.subscribe('market:ticker', featureHandler);

    // StrategyManager stub
    strategyHandler = (features: AnalyticsFeaturesEvent) => {
      const intent: StrategyIntentEvent = {
        intentId: `intent-${features.symbol}-${features.meta.ts}`,
        symbol: features.symbol,
        side: 'LONG',
        targetExposureUsd: 10,
        reason: 'test',
        ts: features.meta.ts,
        meta: inheritMeta(features.meta, 'strategy', { ts: features.meta.ts }),
      };
      bus.publish('strategy:intent', intent);
    };
    bus.subscribe('analytics:features', strategyHandler);

    // RiskManager stub
    riskHandler = (intent: StrategyIntentEvent) => {
      const approved: RiskApprovedIntentEvent = {
        intent,
        approvedAtTs: intent.meta.ts,
        riskVersion: 'v0',
        meta: inheritMeta(intent.meta, 'risk', { ts: intent.meta.ts }),
      };
      bus.publish('risk:approved_intent', approved);
    };
    bus.subscribe('strategy:intent', riskHandler);
  });

  afterEach(() => {
    if (featureHandler) bus.unsubscribe('market:ticker', featureHandler);
    if (strategyHandler) bus.unsubscribe('analytics:features', strategyHandler);
    if (riskHandler) bus.unsubscribe('strategy:intent', riskHandler);
    store.stop();
  });

  it('emits one approved intent with preserved correlationId', async () => {
    const correlationId = 'prod-pipeline-1';
    const ticker: TickerEvent = {
      symbol: 'TESTUSD',
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      lastPrice: '111',
      meta: { ...createMeta('market', { tsEvent: 1, tsIngest: 1, streamId: 'bybit.public.linear.v5' }), correlationId },
    };

    const waitRisk = waitForEvent(bus, 'risk:approved_intent');
    const waitFV = waitForEvent(bus, 'research:featureVectorRecorded');

    bus.publish('market:ticker', ticker);

    const approved = await waitRisk;
    const fv = await waitFV;

    expect(approved.intent.symbol).toBe('TESTUSD');
    expect(approved.meta.correlationId).toBe(correlationId);
    expect(approved.intent.meta.correlationId).toBe(correlationId);
    expect(fv.meta.correlationId).toBe(correlationId);
    expect(Object.keys(fv.features).length).toBeGreaterThanOrEqual(1);
  });
});
