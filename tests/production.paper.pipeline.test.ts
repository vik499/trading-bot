import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createMeta,
  inheritMeta,
  type AnalyticsFeaturesEvent,
  type BotEventMap,
  type ControlState,
  type EventBus,
  type MarketContextEvent,
  type RiskApprovedIntentEvent,
  type StrategyIntentEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { StrategyEngine } from '../src/strategy/StrategyEngine';
import { RiskManager } from '../src/risk/RiskManager';
import { PaperExecutionEngine } from '../src/execution/PaperExecutionEngine';
import { PortfolioManager } from '../src/portfolio/PortfolioManager';

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

describe('Production paper loop', () => {
  const bus = createTestEventBus();
  let strategy: StrategyEngine;
  let risk: RiskManager;
  let exec: PaperExecutionEngine;
  let portfolio: PortfolioManager;

  beforeEach(() => {
    strategy = new StrategyEngine(bus, { momentumThreshold: 0.0001, minIntentIntervalMs: 0, targetExposureUsd: 10 });
    risk = new RiskManager(bus, { cooldownMs: 10_000, maxNotionalUsd: 50, volatilityRejectThreshold: 0.1 });
    exec = new PaperExecutionEngine(bus);
    portfolio = new PortfolioManager(bus);
    strategy.start();
    risk.start();
    exec.start();
    portfolio.start();
  });

  afterEach(() => {
    portfolio.stop();
    exec.stop();
    risk.stop();
    strategy.stop();
  });

  it('analytics -> intent -> approved -> paper fill -> portfolio update', async () => {
    const ctx: MarketContextEvent = {
      symbol: 'BTCUSDT',
      ts: 1_000,
      regime: 'calm',
      volatility: 0.0002,
      featuresReady: true,
      contextReady: true,
      processedCount: 10,
      meta: createMeta('analytics', { ts: 1_000 }),
    };
    bus.publish('analytics:context', ctx);

    const feat: AnalyticsFeaturesEvent = {
      symbol: 'BTCUSDT',
      ts: 1_000,
      lastPrice: 100,
      return1: 0.001,
      sma20: 99,
      volatility: 0.0002,
      momentum: 0.001,
      sampleCount: 25,
      featuresReady: true,
      windowSize: 60,
      smaPeriod: 20,
      meta: { ...createMeta('analytics', { ts: 1_000 }), correlationId: 'cid-123' },
    };

    const waitIntent = waitForEvent(bus, 'strategy:intent');
    const waitApproved = waitForEvent(bus, 'risk:approved_intent');
    const waitFill = waitForEvent(bus, 'exec:paper_fill');
    const waitPortfolio = waitForEvent(bus, 'portfolio:update');

    bus.publish('analytics:features', feat);

    const intent = await waitIntent;
    const approved = await waitApproved;
    const fill = await waitFill;
    const update = await waitPortfolio;

    expect(intent.intentId).toContain('BTCUSDT');
    expect(approved.intent.intentId).toBe(intent.intentId);
    expect(fill.intentId).toBe(intent.intentId);
    expect(fill.fillQty).toBeGreaterThan(0);
    expect(update.qty).toBeGreaterThan(0);

    expect(intent.meta.correlationId).toBe('cid-123');
    expect(approved.meta.correlationId).toBe('cid-123');
    expect(approved.intent.meta.correlationId).toBe('cid-123');
    expect(fill.meta.correlationId).toBe('cid-123');
    expect(update.meta.correlationId).toBe('cid-123');

    expect(intent.meta.ts).toBe(1_000);
    expect(approved.meta.ts).toBe(1_000);
    expect(fill.meta.ts).toBe(1_000);
    expect(update.meta.ts).toBe(1_000);
  });

  it('paused control state rejects intent', async () => {
    const pausedState: ControlState = {
      meta: createMeta('system', { ts: 5 }),
      mode: 'PAPER',
      paused: true,
      lifecycle: 'RUNNING',
      startedAt: 0,
      lastCommandAt: 0,
    };
    bus.publish('control:state', pausedState);

    const intent: StrategyIntentEvent = {
      intentId: 'id-1',
      symbol: 'ETHUSDT',
      side: 'LONG',
      targetExposureUsd: 10,
      reason: 'test',
      ts: 10,
      meta: createMeta('strategy', { ts: 10 }),
    };

    const waitReject = waitForEvent(bus, 'risk:rejected_intent');
    bus.publish('strategy:intent', intent);
    const rejected = await waitReject;

    expect(rejected.reasonCode).toBe('PAUSED');
  });

  it('cooldown rejects rapid consecutive intents', async () => {
    const runningState: ControlState = {
      meta: createMeta('system', { ts: 0 }),
      mode: 'PAPER',
      paused: false,
      lifecycle: 'RUNNING',
      startedAt: 0,
      lastCommandAt: 0,
    };
    bus.publish('control:state', runningState);

    const intentA: StrategyIntentEvent = {
      intentId: 'cool-1',
      symbol: 'SOLUSDT',
      side: 'LONG',
      targetExposureUsd: 10,
      reason: 'a',
      ts: 100,
      meta: createMeta('strategy', { ts: 100 }),
    };
    const intentB: StrategyIntentEvent = {
      intentId: 'cool-2',
      symbol: 'SOLUSDT',
      side: 'LONG',
      targetExposureUsd: 10,
      reason: 'b',
      ts: 105,
      meta: createMeta('strategy', { ts: 105 }),
    };

    const waitApproved = waitForEvent(bus, 'risk:approved_intent');
    const waitReject = waitForEvent(bus, 'risk:rejected_intent');

    bus.publish('strategy:intent', intentA);
    bus.publish('strategy:intent', intentB);

    const approved = await waitApproved;
    const rejected = await waitReject;

    expect(approved.intent.intentId).toBe('cool-1');
    expect(rejected.reasonCode).toBe('COOLDOWN');
  });

  it('paper execution is idempotent for duplicate approvals', async () => {
    const feat: AnalyticsFeaturesEvent = {
      symbol: 'XRPUSDT',
      ts: 200,
      lastPrice: 1,
      return1: 0,
      sma20: 1,
      volatility: 0,
      momentum: 0,
      sampleCount: 10,
      featuresReady: true,
      windowSize: 10,
      smaPeriod: 5,
      meta: createMeta('analytics', { ts: 200 }),
    };
    bus.publish('analytics:features', feat);

    const intent: StrategyIntentEvent = {
      intentId: 'dup-1',
      symbol: 'XRPUSDT',
      side: 'LONG',
      targetExposureUsd: 5,
      reason: 'dup',
      ts: 200,
      meta: createMeta('strategy', { ts: 200 }),
    };
    const approved: RiskApprovedIntentEvent = {
      intent,
      approvedAtTs: 200,
      riskVersion: 'v0',
      meta: inheritMeta(intent.meta, 'risk', { ts: intent.meta.ts }),
    };

    const fills: RiskApprovedIntentEvent[] = [];
    bus.subscribe('exec:paper_fill', (f) => fills.push(f as any));

    bus.publish('risk:approved_intent', approved);
    bus.publish('risk:approved_intent', approved);

    await new Promise((r) => setTimeout(r, 50));
    expect(fills.length).toBe(1);
  });
});
