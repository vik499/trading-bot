import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createMeta, inheritMeta, type BotEventMap, type FeaturesComputed, type TickerEvent, EventBus } from '../src/core/events/EventBus';
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

const makeFeatureEngine = (bus: EventBus) => {
  const handler = (ticker: TickerEvent) => {
    const features: FeaturesComputed = {
      symbol: ticker.symbol,
      timeframe: '1m',
      features: {
        price: Number(ticker.lastPrice ?? 0),
        emaFast: Number(ticker.lastPrice ?? 0),
      },
      meta: inheritMeta(ticker.meta, 'analytics'),
    };
    bus.publish('analytics:features', features);
  };

  bus.subscribe('market:ticker', handler);
  return () => bus.unsubscribe('market:ticker', handler);
};

let bus: EventBus;
let cleanupFns: Array<() => void> = [];
let store: InMemoryFeatureStore;

beforeEach(() => {
  bus = createTestEventBus();
  cleanupFns = [];
  cleanupFns.push(makeFeatureEngine(bus));
  store = new InMemoryFeatureStore(100, bus);
  store.start();
});

afterEach(() => {
  cleanupFns.forEach((fn) => fn());
  cleanupFns = [];
  store.stop();
});

describe('EventBus pipeline market -> analytics -> research', () => {
  it('publishes feature vector once for a market ticker', async () => {
    const ticker: TickerEvent = {
      symbol: 'TESTUSD',
      lastPrice: '101',
      meta: createMeta('market'),
    };

    const wait = waitForEvent(bus, 'research:featureVectorRecorded');
    bus.publish('market:ticker', ticker);
    const recorded = await wait;

    expect(recorded.symbol).toBe('TESTUSD');
    expect(recorded.meta.source).toBe('research');
    expect(typeof recorded.meta.ts).toBe('number');
    expect(Object.keys(recorded.features).length).toBeGreaterThanOrEqual(1);
    expect(recorded.featureVersion).toBe('v1');
  });

  it('preserves correlationId through analytics -> research chain', async () => {
    const ticker: TickerEvent = {
      symbol: 'TESTUSD',
      lastPrice: '202',
      meta: { ...createMeta('market'), correlationId: 'corr-123' },
    };

    const wait = waitForEvent(bus, 'research:featureVectorRecorded');
    bus.publish('market:ticker', ticker);
    const recorded = await wait;

    expect(recorded.meta.correlationId).toBe('corr-123');
  });
});
