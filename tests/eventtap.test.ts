import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import {
  createMeta,
  inheritMeta,
  type AnalyticsFeaturesEvent,
  type AnalyticsReadyEvent,
  type MarketContextEvent,
  type PaperFillEvent,
  type PortfolioUpdateEvent,
  type RiskApprovedIntentEvent,
  type StrategyIntentEvent,
  type TickerEvent,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { EventTap } from '../src/observability/EventTap';
import { logger, type LogLevel } from '../src/infra/logger';

describe('EventTap', () => {
  let logs: string[];
  let prevLevel: LogLevel;
  let prevDisplay: boolean;

  beforeEach(() => {
    logs = [];
    prevLevel = logger.getLevel();
    prevDisplay = logger.isDisplayEnabled();
    logger.setLevel('info');
    logger.setDisplay(true);
    logger.setSink((_entry, formatted) => {
      logs.push(formatted);
    });
  });

  afterEach(() => {
    logger.resetSinkToConsole();
    logger.setLevel(prevLevel);
    logger.setDisplay(prevDisplay);
  });

  it('logs analytics ready once per symbol', () => {
    const bus = createTestEventBus();
    const tap = new EventTap(bus, { summaryIntervalMs: 0 });
    tap.start();

    const readyA: AnalyticsReadyEvent = {
      symbol: 'BTCUSDT',
      ts: 1000,
      tf: '5m',
      ready: true,
      reason: 'klineWarmup',
      meta: createMeta('analytics', { ts: 1000 }),
    };
    const readyB: AnalyticsReadyEvent = {
      symbol: 'BTCUSDT',
      ts: 1100,
      tf: '5m',
      ready: true,
      reason: 'klineWarmup',
      meta: createMeta('analytics', { ts: 1100 }),
    };

    bus.publish('analytics:ready', readyA);
    bus.publish('analytics:ready', readyB);
    tap.stop();

    const readyLogs = logs.filter((line) => line.includes('[Analytics] READY') && line.includes('symbol=BTCUSDT'));
    expect(readyLogs).toHaveLength(1);
  });

  it('aggregates counters and throttles summary logs', () => {
    const bus = createTestEventBus();
    const tap = new EventTap(bus, { summaryIntervalMs: 0, summaryThrottleMs: 1000 });
    tap.start();

    const ticker: TickerEvent = {
      symbol: 'ETHUSDT',
      streamId: 'bybit.public.linear.v5',
      marketType: 'futures',
      lastPrice: '100',
      meta: createMeta('market', {
        tsEvent: 1000,
        tsIngest: 1000,
        streamId: 'bybit.public.linear.v5',
        correlationId: 'cid-1',
      }),
    };
    bus.publish('market:ticker', ticker);

    const features: AnalyticsFeaturesEvent = {
      symbol: 'ETHUSDT',
      ts: 1000,
      lastPrice: 100,
      sampleCount: 20,
      featuresReady: true,
      windowSize: 60,
      smaPeriod: 20,
      meta: createMeta('analytics', { ts: 1000, correlationId: 'cid-1' }),
    };
    bus.publish('analytics:features', features);

    const context: MarketContextEvent = {
      symbol: 'ETHUSDT',
      ts: 1000,
      regime: 'calm',
      volatility: 0.0002,
      featuresReady: true,
      contextReady: true,
      processedCount: 5,
      meta: createMeta('analytics', { ts: 1000, correlationId: 'cid-1' }),
    };
    bus.publish('analytics:context', context);

    const intent: StrategyIntentEvent = {
      intentId: 'intent-1',
      symbol: 'ETHUSDT',
      side: 'LONG',
      targetExposureUsd: 10,
      reason: 'test',
      ts: 1000,
      meta: createMeta('strategy', { ts: 1000, correlationId: 'cid-1' }),
    };
    bus.publish('strategy:intent', intent);

    const approved: RiskApprovedIntentEvent = {
      intent,
      approvedAtTs: 1000,
      riskVersion: 'v0',
      meta: inheritMeta(intent.meta, 'risk', { ts: intent.meta.ts }),
    };
    bus.publish('risk:approved_intent', approved);

    const fill: PaperFillEvent = {
      intentId: intent.intentId,
      symbol: intent.symbol,
      side: intent.side,
      fillPrice: 100,
      fillQty: 0.1,
      notionalUsd: 10,
      ts: 1000,
      meta: inheritMeta(approved.meta, 'trading', { ts: approved.meta.ts }),
    };
    bus.publish('exec:paper_fill', fill);

    const update: PortfolioUpdateEvent = {
      symbol: 'ETHUSDT',
      qty: 0.1,
      avgPrice: 100,
      realizedPnl: 0,
      updatedAtTs: 1000,
      meta: inheritMeta(fill.meta, 'portfolio', { ts: fill.meta.ts }),
    };
    bus.publish('portfolio:update', update);

    const counters = tap.getCounters();
    expect(counters.ticksSeen).toBe(1);
    expect(counters.featuresEmitted).toBe(1);
    expect(counters.contextsEmitted).toBe(1);
    expect(counters.intents).toBe(1);
    expect(counters.approvals).toBe(1);
    expect(counters.rejections).toBe(0);
    expect(counters.fills).toBe(1);
    expect(counters.portfolioUpdates).toBe(1);

    tap.emitSummary();
    tap.emitSummary();

    const summaryLogs = logs.filter((line) => line.includes('[EventTap] counters'));
    expect(summaryLogs).toHaveLength(1);
    expect(summaryLogs[0]).toContain('ticks=1');
    expect(summaryLogs[0]).toContain('intents=1');
    expect(summaryLogs[0]).toContain('approvals=1');
    expect(summaryLogs[0]).toContain('fills=1');

    const intentLogs = logs.filter((line) => line.includes('[Strategy] first intent') && line.includes('symbol=ETHUSDT'));
    const riskLogs = logs.filter((line) => line.includes('[Risk] first decision') && line.includes('symbol=ETHUSDT'));
    const fillLogs = logs.filter((line) => line.includes('[Exec] first fill') && line.includes('symbol=ETHUSDT'));
    expect(intentLogs).toHaveLength(1);
    expect(riskLogs).toHaveLength(1);
    expect(fillLogs).toHaveLength(1);

    tap.stop();
  });

  it('does not read payload.meta', () => {
    const filePath = path.join(__dirname, '..', 'src', 'observability', 'EventTap.ts');
    const content = fs.readFileSync(filePath, 'utf8');
    expect(content).not.toMatch(/payload\\s*\\.\\s*meta|payload\\?\\.meta|\\.payload\\.meta/);
  });
});
