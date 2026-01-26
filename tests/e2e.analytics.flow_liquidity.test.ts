import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import { TestRuntime } from './helpers/testRuntime';
import { FakeClock } from './helpers/fakeClock';
import { createMeta } from '../src/core/events/EventBus';

const SYMBOL = 'BTCUSDT';

describe('E2E analytics wiring', () => {
  let runtime: TestRuntime;

  beforeEach(() => {
    runtime = new TestRuntime({
      baseDir: './data/test-runtime',
      startJournal: false,
      startAnalytics: true,
      startStrategy: false,
      startRisk: false,
      startExecution: false,
      startPortfolio: false,
      startMetrics: false,
    }, new FakeClock(1_700_000_000_000, 1000));
  });

  afterEach(async () => {
    await runtime.stop();
  });

  it('FlowEngine emits analytics:flow with deterministic meta and correlationId', () => {
    const corr = 'flow-corr-1';
    runtime.publishMarketTrade({
      side: 'Buy',
      price: 100,
      size: 2,
      ts: 1_700_000_000_000,
      tradeTs: 1_700_000_000_000 - 200,
      exchangeTs: 1_700_000_000_000 - 200,
      correlationId: corr,
    });
    runtime.publishMarketOi({
      openInterest: 2000,
      ts: 1_700_000_000_500,
      exchangeTs: 1_700_000_000_400,
      correlationId: corr,
    });
    runtime.publishMarketFunding({
      fundingRate: 0.00015,
      ts: 1_700_000_001_000,
      exchangeTs: 1_700_000_000_900,
      correlationId: corr,
    });

    const flows = runtime.getEvents('analytics:flow');
    expect(flows.length).toBeGreaterThan(0);
    flows.forEach((evt) => {
      expect(evt.meta.ts).toBe(evt.ts);
      expect(evt.meta.correlationId).toBe(corr);
    });
  });

  it('LiquidityEngine emits analytics:liquidity with deterministic meta and correlationId', () => {
    const corr = 'liq-corr-1';
    runtime.publishMarketOrderbookL2({
      kind: 'snapshot',
      updateId: 1,
      ts: 1_700_000_010_000,
      exchangeTs: 1_700_000_009_500,
      bids: [{ price: 100, size: 2 }],
      asks: [{ price: 101, size: 3 }],
      correlationId: corr,
    });

    const liq = runtime.getEvents('analytics:liquidity');
    expect(liq.length).toBeGreaterThan(0);
    liq.forEach((evt) => {
      expect(evt.meta.ts).toBe(evt.ts);
      expect(evt.meta.correlationId).toBe(corr);
    });
  });

  it('Feature and Kline features emit analytics:features', () => {
    runtime.publishMarketTicker({ lastPrice: '100', ts: 1_700_000_020_000, exchangeTs: 1_700_000_020_000 });
    runtime.publishMarketKline({ tf: '5m', endTs: 1_700_000_300_000, close: 101 });

    const features = runtime.getEvents('analytics:features');
    expect(features.length).toBeGreaterThan(0);
    const hasTicker = features.some((evt) => evt.sourceTopic === 'market:ticker');
    const hasKline = features.some((evt) => evt.sourceTopic === 'market:kline');
    expect(hasTicker).toBe(true);
    expect(hasKline).toBe(true);
  });

  it('MarketContextBuilder emits macro ready and context after MTF warmup', () => {
    const corr = 'macro-corr-1';
    const warmupCount = 26;
    const publishSeries = (tf: '1h' | '4h' | '1d', startTs: number) => {
      const intervalMs = tf === '1h' ? 3_600_000 : tf === '4h' ? 14_400_000 : 86_400_000;
      for (let i = 0; i < warmupCount; i += 1) {
        const endTs = startTs + intervalMs * (i + 1);
        runtime.publishMarketKline({ tf, endTs, close: 100 + i, correlationId: corr });
      }
    };

    publishSeries('1h', 1_700_000_000_000);
    publishSeries('4h', 1_700_100_000_000);
    publishSeries('1d', 1_700_200_000_000);

    const ready = runtime.getEvents('analytics:ready').filter((evt) => evt.reason === 'macroWarmup');
    expect(ready.length).toBeGreaterThan(0);
    const macroReady = ready[ready.length - 1];
    expect(macroReady.readyTfs).toEqual(expect.arrayContaining(['1h', '4h', '1d']));

    runtime.publishMarketKline({ tf: '1h', endTs: 1_700_000_000_000 + 3_600_000 * (warmupCount + 1), close: 200, correlationId: corr });

    const contexts = runtime.getEvents('analytics:context');
    const macroContexts = contexts.filter((evt) => evt.tf === 'macro');
    expect(macroContexts.length).toBeGreaterThan(0);
  });

  it('paper loop preserves deterministic meta.ts across approvals and fills', async () => {
    const paperRuntime = new TestRuntime({
      baseDir: './data/test-runtime-paper',
      startJournal: false,
      startAnalytics: false,
      startStrategy: true,
      startRisk: true,
      startExecution: true,
      startPortfolio: true,
      startMetrics: false,
    }, new FakeClock(1_700_000_000_000, 1000));

    const corr = 'paper-corr-1';
    const meta = createMeta('analytics', { ts: 1_700_000_050_000, correlationId: corr });

    paperRuntime.bus.publish('analytics:context', {
      symbol: SYMBOL,
      ts: meta.ts,
      regime: 'calm',
      volatility: 0.0001,
      featuresReady: true,
      contextReady: true,
      processedCount: 1,
      meta,
    });

    paperRuntime.bus.publish('analytics:features', {
      symbol: SYMBOL,
      ts: meta.ts,
      lastPrice: 100,
      momentum: 0.01,
      sampleCount: 2,
      featuresReady: true,
      windowSize: 2,
      smaPeriod: 2,
      meta,
      sourceTopic: 'market:ticker',
    });

    const intents = paperRuntime.getEvents('strategy:intent');
    const approved = paperRuntime.getEvents('risk:approved_intent');
    const fills = paperRuntime.getEvents('exec:paper_fill');
    const updates = paperRuntime.getEvents('portfolio:update');

    expect(intents.length).toBe(1);
    expect(approved.length).toBe(1);
    expect(fills.length).toBe(1);
    expect(updates.length).toBe(1);

    expect(intents[0].meta.ts).toBe(meta.ts);
    expect(approved[0].meta.ts).toBe(meta.ts);
    expect(fills[0].meta.ts).toBe(meta.ts);
    expect(updates[0].meta.ts).toBe(meta.ts);

    expect(intents[0].meta.correlationId).toBe(corr);
    expect(approved[0].meta.correlationId).toBe(corr);
    expect(fills[0].meta.correlationId).toBe(corr);
    expect(updates[0].meta.correlationId).toBe(corr);

    await paperRuntime.stop();
  });
});
