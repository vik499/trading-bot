import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createTestEventBus } from '../../src/core/events/testing';
import {
  createMeta,
  type DataGapDetected,
  type MarketCvdAggEvent,
  type MarketCvdEvent,
} from '../../src/core/events/EventBus';
import { SourceRegistry, type SourceRegistrySnapshot } from '../../src/core/market/SourceRegistry';
import { CvdAggregator } from '../../src/globalData/CvdAggregator';
import { logger } from '../../src/infra/logger';
import { MarketDataReadiness } from '../../src/observability/MarketDataReadiness';

const DEBUG_ENVS = ['BOT_CVD_DEBUG', 'BOT_FLOW_DEBUG', 'BOT_READINESS_DEBUG', 'BOT_GAP_DEBUG'] as const;
const DEBUG_PREFIXES = ['[CvdDebug]', '[FlowDebug]', '[ReadinessDebug]', '[GapDebug]'];

const makeCvdEvent = (streamId: string, ts: number, total: number, delta: number): MarketCvdEvent => ({
  symbol: 'BTCUSDT',
  streamId,
  marketType: 'spot',
  bucketStartTs: 0,
  bucketEndTs: 1000,
  bucketSizeMs: 1000,
  cvdTotal: total,
  cvdDelta: delta,
  unit: 'base',
  exchangeTs: ts,
  meta: createMeta('market', { tsEvent: ts, tsIngest: ts, streamId }),
});

const makeCvdAggEvent = (bucketTs: number, mismatchDetected: boolean): MarketCvdAggEvent => ({
  symbol: 'BTCUSDT',
  ts: bucketTs,
  cvd: 100,
  cvdDelta: 10,
  bucketStartTs: bucketTs - 1000,
  bucketEndTs: bucketTs,
  bucketSizeMs: 1000,
  unit: 'base',
  marketType: 'spot',
  sourcesUsed: ['binance.usdm.public'],
  mismatchDetected,
  confidenceScore: 0.5,
  meta: createMeta('global_data', { tsEvent: bucketTs, tsIngest: bucketTs }),
});

const makeGapEvent = (): DataGapDetected => ({
  symbol: 'BTCUSDT',
  streamId: 'binance.usdm.public',
  topic: 'market:orderbook_l2_delta',
  prevTsExchange: 1000,
  currTsExchange: 2000,
  deltaMs: 1000,
  meta: createMeta('storage', { tsEvent: 2000, tsIngest: 2000 }),
});

describe.sequential('Debug flags', () => {
  let envSnapshot: Record<string, string | undefined> = {};

  beforeEach(() => {
    envSnapshot = {};
    for (const key of DEBUG_ENVS) {
      envSnapshot[key] = process.env[key];
      delete process.env[key];
    }
  });

  afterEach(() => {
    for (const key of DEBUG_ENVS) {
      const value = envSnapshot[key];
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });

  it('keeps debug logging off by default', () => {
    const infoSpy = vi.spyOn(logger, 'info');
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
      now: () => 1000,
    });
    const cvdAgg = new CvdAggregator(bus, { ttlMs: 10_000 });

    readiness.start();
    cvdAgg.start();

    bus.publish('market:cvd_spot', makeCvdEvent('binance.usdm.public', 1000, 100, 10));
    bus.publish('market:cvd_spot_agg', makeCvdAggEvent(1000, true));
    bus.publish('data:gapDetected', makeGapEvent());

    const debugLines = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && DEBUG_PREFIXES.some((prefix) => msg.includes(prefix)));

    expect(debugLines).toHaveLength(0);

    cvdAgg.stop();
    readiness.stop();
    infoSpy.mockRestore();
  });

  it('emits CvdDebug once per bucket when enabled', () => {
    process.env.BOT_CVD_DEBUG = '1';
    const infoSpy = vi.spyOn(logger, 'info');
    const bus = createTestEventBus();
    const cvdAgg = new CvdAggregator(bus, { ttlMs: 10_000 });

    cvdAgg.start();

    bus.publish('market:cvd_spot', makeCvdEvent('binance.usdm.public', 1000, 100, 10));
    bus.publish('market:cvd_spot', makeCvdEvent('binance.usdm.public', 1000, 110, 5));

    const debugLines = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('[CvdDebug]'));

    expect(debugLines.length).toBe(1);

    cvdAgg.stop();
    infoSpy.mockRestore();
  });

  it('emits Flow/Readiness/Gap debug lines when enabled', () => {
    process.env.BOT_FLOW_DEBUG = '1';
    process.env.BOT_READINESS_DEBUG = '1';
    process.env.BOT_GAP_DEBUG = '1';

    const infoSpy = vi.spyOn(logger, 'info');
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
      now: () => 1000,
    });

    readiness.start();

    bus.publish('market:cvd_spot_agg', makeCvdAggEvent(1000, true));
    bus.publish('data:gapDetected', makeGapEvent());

    const logs = infoSpy.mock.calls.map((call) => call[0]).filter((msg) => typeof msg === 'string');

    expect(logs.some((msg) => msg.includes('[FlowDebug]'))).toBe(true);
    expect(logs.some((msg) => msg.includes('[ReadinessDebug]'))).toBe(true);
    expect(logs.some((msg) => msg.includes('[GapDebug]'))).toBe(true);

    readiness.stop();
    infoSpy.mockRestore();
  });

  it('dedups FlowDebug by symbol+marketType+bucketTs', () => {
    process.env.BOT_FLOW_DEBUG = '1';
    const infoSpy = vi.spyOn(logger, 'info');
    const bus = createTestEventBus();
    const registry = new SourceRegistry();
    const readiness = new MarketDataReadiness(bus, {
      sourceRegistry: registry,
      bucketMs: 1000,
      warmingWindowMs: 0,
      expectedFlowTypes: ['spot'],
      expectedDerivativeKinds: [],
    });

    const snapshot: SourceRegistrySnapshot = {
      symbol: 'BTCUSDT',
      marketType: 'spot',
      expected: { price: [], flow: [], liquidity: [], derivatives: [] },
      usedAgg: { price: [], flow: [], liquidity: [], derivatives: [] },
      usedRaw: { trades: [], orderbook: [], oi: [], funding: [], markPrice: [], indexPrice: [], klines: [] },
      lastSeenRawTs: {
        trades: null,
        orderbook: null,
        oi: null,
        funding: null,
        markPrice: null,
        indexPrice: null,
        klines: null,
      },
      lastSeenAggTs: {
        priceCanonical: null,
        flowAgg: null,
        liquidityAgg: null,
        derivativesAgg: null,
      },
      suppressions: {
        price: { count: 0, reasons: {} },
        flow: { count: 0, reasons: {} },
        liquidity: { count: 0, reasons: {} },
        derivatives: { count: 0, reasons: {} },
      },
      nonMonotonicSources: [],
    };

    const readinessInternal = readiness as unknown as {
      computeBlockConfidence: (bucketTs: number, snapshot: SourceRegistrySnapshot) => Record<string, number>;
      lastSymbol?: string;
      lastMarketType?: string;
    };

    readinessInternal.lastSymbol = 'BTCUSDT';
    readinessInternal.lastMarketType = 'spot';
    readinessInternal.computeBlockConfidence(1000, snapshot);

    readinessInternal.lastSymbol = 'ETHUSDT';
    readinessInternal.lastMarketType = 'spot';
    readinessInternal.computeBlockConfidence(1000, snapshot);

    const flowLogs = infoSpy.mock.calls
      .map((call) => call[0])
      .filter((msg) => typeof msg === 'string' && msg.includes('[FlowDebug]'));

    expect(flowLogs.length).toBe(1);

    infoSpy.mockRestore();
  });
});
