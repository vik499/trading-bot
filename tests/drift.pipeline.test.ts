import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { createMeta, type BotEventMap, type FeatureVectorRecorded, EventBus } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { DriftDetector } from '../src/core/research/DriftDetector';

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

describe('DriftDetector pipeline research:featureVectorRecorded -> analytics:marketDriftDetected', () => {
  const baselineValue = 0.01;
  const shiftedValue = 0.2;
  let bus: EventBus;
  let drift: DriftDetector;

  beforeEach(() => {
    bus = createTestEventBus();
    drift = new DriftDetector({
      enabled: true,
      baselineSize: 3,
      windowSize: 3,
      meanDeltaThreshold: 0.05,
      minEmitIntervalMs: 0,
      eventBus: bus,
    });
    drift.start();
  });

  afterEach(() => {
    drift.stop();
  });

  it('emits drift once with preserved correlationId and stats', async () => {
    const correlationId = 'drift-corr-1';

    const makeFV = (val: number): FeatureVectorRecorded => ({
      symbol: 'TESTUSD',
      timeframe: '1m',
      featureVersion: 'v1',
      features: { ret: val },
      meta: { ...createMeta('research'), correlationId },
    });

    // Feed baseline
    [baselineValue, baselineValue, baselineValue].forEach((v) => {
      bus.publish('research:featureVectorRecorded', makeFV(v));
    });

    const wait = waitForEvent(bus, 'analytics:marketDriftDetected');

    // Feed shifted window
    [shiftedValue, shiftedValue, shiftedValue].forEach((v) => {
      bus.publish('research:featureVectorRecorded', makeFV(v));
    });

    const driftEvt = await wait;

    expect(driftEvt.symbol).toBe('TESTUSD');
    expect(driftEvt.meta.correlationId).toBe(correlationId);
    expect(driftEvt.window.currentSize).toBeGreaterThanOrEqual(3);
    expect(driftEvt.window.baselineSize).toBeGreaterThanOrEqual(3);
    const statsKeys = Object.keys(driftEvt.stats);
    expect(statsKeys.length).toBeGreaterThanOrEqual(1);
    const first = driftEvt.stats[statsKeys[0]];
    expect(typeof first.meanDelta).toBe('number');
    expect(Math.abs(first.meanDelta ?? 0)).toBeGreaterThan(0.05);
  });
});
