import { describe, expect, it } from 'vitest';
import {
  createMeta,
  type BotEventMap,
  type EventBus,
  type ReplayFinished,
  type ReplayStarted,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { BacktestMetricsCollector } from '../src/metrics/BacktestMetricsCollector';

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

describe('BacktestMetricsCollector', () => {
  it('computes metrics and emits summary with deterministic meta.ts', async () => {
    const bus = createTestEventBus();
    const collector = new BacktestMetricsCollector(bus);
    collector.start();

    const start: ReplayStarted = {
      streamId: 'run-1',
      symbol: 'TESTUSD',
      filesCount: 1,
      mode: 'max',
      emittedCount: 0 as any, // not used
      meta: createMeta('replay', { ts: 1000 }),
    } as any;

    bus.publish('replay:started', start);

    bus.publish('market:ticker', {
      symbol: 'TESTUSD',
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      lastPrice: '100',
      meta: createMeta('market', { tsEvent: 1100, tsIngest: 1100, streamId: 'bybit.public.linear.v5' }),
    });
    bus.publish('market:ticker', {
      symbol: 'TESTUSD',
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      lastPrice: '101',
      meta: createMeta('market', { tsEvent: 1400, tsIngest: 1400, streamId: 'bybit.public.linear.v5' }),
    });

    // realized pnl path: +10 -> +15 -> -5 -> -2 (max dd from peak 15 to -2 = 17)
    bus.publish('portfolio:update', {
      symbol: 'TESTUSD',
      qty: 1,
      avgPrice: 100,
      realizedPnl: 10,
      updatedAtTs: 1100,
      meta: createMeta('portfolio', { ts: 1100 }),
    });
    bus.publish('portfolio:update', {
      symbol: 'TESTUSD',
      qty: 1,
      avgPrice: 100,
      realizedPnl: 15,
      updatedAtTs: 1200,
      meta: createMeta('portfolio', { ts: 1200 }),
    });
    bus.publish('portfolio:update', {
      symbol: 'TESTUSD',
      qty: 1,
      avgPrice: 100,
      realizedPnl: -5,
      updatedAtTs: 1300,
      meta: createMeta('portfolio', { ts: 1300 }),
    });
    bus.publish('portfolio:update', {
      symbol: 'TESTUSD',
      qty: 1,
      avgPrice: 100,
      realizedPnl: -2,
      updatedAtTs: 1400,
      meta: createMeta('portfolio', { ts: 1400 }),
    });

    bus.publish('exec:paper_fill', {
      intentId: 'i1',
      symbol: 'TESTUSD',
      side: 'LONG',
      fillPrice: 100,
      fillQty: 1,
      notionalUsd: 100,
      ts: 1100,
      meta: createMeta('trading', { ts: 1100 }),
    });
    bus.publish('exec:paper_fill', {
      intentId: 'i2',
      symbol: 'TESTUSD',
      side: 'LONG',
      fillPrice: 102,
      fillQty: 1,
      notionalUsd: 102,
      ts: 1200,
      meta: createMeta('trading', { ts: 1200 }),
    });
    bus.publish('exec:paper_fill', {
      intentId: 'i3',
      symbol: 'TESTUSD',
      side: 'SHORT',
      fillPrice: 101,
      fillQty: 1,
      notionalUsd: 101,
      ts: 1300,
      meta: createMeta('trading', { ts: 1300 }),
    });

    const finished: ReplayFinished = {
      streamId: 'run-1',
      symbol: 'TESTUSD',
      emittedCount: 0,
      warningCount: 0,
      durationMs: 0,
      filesProcessed: 1,
      meta: createMeta('replay', { ts: 5000 }),
    } as any;

    const waitSummary = waitForEvent(bus, 'metrics:backtest_summary');
    bus.publish('replay:finished', finished);
    const summary = await waitSummary;

    expect(summary.totalFills).toBe(3);
    expect(summary.realizedPnl).toBe(-2);
    expect(summary.maxDrawdown).toBe(20);
    expect(summary.startTs).toBe(1100);
    expect(summary.endTs).toBe(1400);
    expect(summary.meta.ts).toBe(5000);
    expect(summary.meta.source).toBe('metrics');

    collector.stop();
  });
});
