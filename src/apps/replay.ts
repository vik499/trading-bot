import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import { eventBus } from '../core/events/EventBus';
import { createJournalReplayRunner, type ReplayMode } from '../replay/JournalReplayRunner';
import { WalkForwardRunner } from '../replay/WalkForwardRunner';
import { FeatureEngine, KlineFeatureEngine } from '../analytics/FeatureEngine';
import { FlowEngine } from '../analytics/FlowEngine';
import { LiquidityEngine } from '../analytics/LiquidityEngine';
import { MarketContextBuilder } from '../analytics/MarketContextBuilder';
import { MarketViewBuilder } from '../analytics/MarketViewBuilder';
import { RegimeEngineV1 } from '../analytics/RegimeEngineV1';
import { StrategyEngine } from '../strategy/StrategyEngine';
import { RiskManager } from '../risk/RiskManager';
import { PaperExecutionEngine } from '../execution/PaperExecutionEngine';
import { PortfolioManager } from '../portfolio/PortfolioManager';
import { SnapshotCoordinator } from '../state/SnapshotCoordinator';
import { BacktestMetricsCollector } from '../metrics/BacktestMetricsCollector';
import { EventTap } from '../observability/EventTap';
import { GlobalDataGateway } from '../globalData/GlobalDataGateway';
import { GlobalDataQualityMonitor } from '../globalData/GlobalDataQualityMonitor';
import { OpenInterestAggregator } from '../globalData/OpenInterestAggregator';
import { FundingAggregator } from '../globalData/FundingAggregator';
import { CvdCalculator } from '../globalData/CvdCalculator';
import { CvdAggregator } from '../globalData/CvdAggregator';
import { LiquidationAggregator } from '../globalData/LiquidationAggregator';
import { LiquidityAggregator } from '../globalData/LiquidityAggregator';
import { CanonicalPriceAggregator } from '../globalData/CanonicalPriceAggregator';
import { PriceIndexAggregator } from '../globalData/PriceIndexAggregator';
import { PatternMiner } from '../research/PatternMiner';
import { MarketDataReadiness } from '../observability/MarketDataReadiness';

interface CliArgs {
  streamId: string;
  symbol: string;
  dateFrom?: string;
  dateTo?: string;
  runId?: string;
  mode: ReplayMode;
  speedFactor: number;
  walkForward: boolean;
  trainDays: number;
  testDays: number;
  stepDays: number;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const lookup = new Map<string, string>();
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.replace(/^--/, '');
      const val = args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : 'true';
      lookup.set(key, val);
      if (val !== 'true') i += 1;
    }
  }

  const streamId = lookup.get('streamId') ?? lookup.get('stream') ?? process.env.BOT_STREAM_ID;
  const symbol = lookup.get('symbol') ?? process.env.BOT_SYMBOL;
  if (!streamId || !symbol) {
    console.error('Usage: ts-node src/apps/replay.ts --streamId <id> --symbol <symbol> [--runId <id>] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--mode max|accelerated|realtime] [--speed 10] [--walkforward true] [--trainDays 30] [--testDays 10] [--stepDays 10]');
    process.exit(1);
  }

  const mode = (lookup.get('mode') as ReplayMode | undefined) ?? 'max';
  const speedFactor = Number(lookup.get('speed') ?? lookup.get('speedFactor') ?? 10);
  const walkForward = (lookup.get('walkforward') ?? lookup.get('walkForward')) === 'true';
  const trainDays = Number(lookup.get('trainDays') ?? 30);
  const testDays = Number(lookup.get('testDays') ?? 10);
  const stepDays = Number(lookup.get('stepDays') ?? 10);

  return {
    streamId,
    symbol,
    dateFrom: lookup.get('from') ?? lookup.get('dateFrom'),
    dateTo: lookup.get('to') ?? lookup.get('dateTo'),
    runId: lookup.get('runId') ?? process.env.BOT_RUN_ID,
    mode,
    speedFactor: Number.isFinite(speedFactor) && speedFactor > 0 ? speedFactor : 10,
    walkForward,
    trainDays: Number.isFinite(trainDays) && trainDays > 0 ? trainDays : 30,
    testDays: Number.isFinite(testDays) && testDays > 0 ? testDays : 10,
    stepDays: Number.isFinite(stepDays) && stepDays > 0 ? stepDays : 10,
  };
}

function parseIntervalMs(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNumber(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseWeights(raw: string | undefined): Record<string, number> {
  if (!raw) return {};
  const weights: Record<string, number> = {};
  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const [key, value] = trimmed.split(':');
    if (!key || !value) continue;
    const weight = Number(value);
    if (!Number.isFinite(weight)) continue;
    weights[key.trim()] = weight;
  }
  return weights;
}

function getGlobalTtlMs(): number {
  return parseIntervalMs(process.env.BOT_GLOBAL_TTL_MS, 120_000);
}

function getGlobalTtlPolicy(): {
  oiMs: number;
  fundingMs: number;
  cvdMs: number;
  liquidationsMs: number;
  liquidityMs: number;
  priceMs: number;
} {
  const globalTtlMs = getGlobalTtlMs();
  return {
    oiMs: parseIntervalMs(process.env.BOT_OI_TTL_MS, globalTtlMs),
    fundingMs: parseIntervalMs(process.env.BOT_FUNDING_TTL_MS, globalTtlMs),
    cvdMs: parseIntervalMs(process.env.BOT_CVD_TTL_MS, globalTtlMs),
    liquidationsMs: parseIntervalMs(process.env.BOT_LIQUIDATIONS_TTL_MS, globalTtlMs),
    liquidityMs: parseIntervalMs(process.env.BOT_LIQUIDITY_TTL_MS, globalTtlMs),
    priceMs: parseIntervalMs(process.env.BOT_PRICE_TTL_MS, globalTtlMs),
  };
}

function getCvdBucketMs(): number {
  return parseIntervalMs(process.env.BOT_CVD_BUCKET_MS, 5_000);
}

function getLiqBucketMs(): number {
  return parseIntervalMs(process.env.BOT_LIQ_BUCKET_MS, 10_000);
}

function getLiquidityDepthLevels(): number {
  return Math.max(1, Math.floor(parseNumber(process.env.BOT_LIQUIDITY_DEPTH_LEVELS, 10)));
}

function getReadinessBucketMs(): number {
  return parseIntervalMs(process.env.BOT_READINESS_BUCKET_MS, 1_000);
}

function getReadinessWarmupMs(): number {
  return parseIntervalMs(process.env.BOT_READINESS_WARMUP_MS, 30 * 60_000);
}

function getExpectedSources(): number {
  return Math.max(0, Math.floor(parseNumber(process.env.BOT_EXPECTED_SOURCES, 0)));
}

function getReadinessLogMs(): number {
  return parseIntervalMs(process.env.BOT_READINESS_LOG_MS, 10_000);
}

async function main(): Promise<void> {
  const args = parseArgs();
  logger.info(m('lifecycle', `[Replay] starting: stream=${args.streamId} symbol=${args.symbol} mode=${args.mode} speed=${args.speedFactor}`));

  const tap = new EventTap(eventBus, { summaryIntervalMs: 0 });
  tap.start();

  const runner = createJournalReplayRunner(eventBus, {
    streamId: args.streamId,
    symbol: args.symbol,
    dateFrom: args.dateFrom,
    dateTo: args.dateTo,
    runId: args.runId,
    mode: args.mode,
    speedFactor: args.speedFactor,
  });

  eventBus.subscribe('replay:warning', (evt) => {
    logger.warn(m('warn', `[Replay] warning file=${evt.file} line=${evt.lineNumber}: ${evt.reason}`));
  });
  eventBus.subscribe('replay:error', (evt) => {
    logger.error(m('error', `[Replay] error file=${evt.file ?? 'n/a'}: ${evt.message}`));
  });
  eventBus.subscribe('replay:progress', (evt) => {
    logger.infoThrottled('replay:progress', m('ok', `[Replay] progress emitted=${evt.emittedCount} warnings=${evt.warningsCount} file=${evt.currentFile ?? 'n/a'}`), 2000);
  });
  eventBus.subscribe('replay:finished', (evt) => {
    logger.info(m('ok', `[Replay] finished emitted=${evt.emittedCount} warnings=${evt.warningCount} durationMs=${evt.durationMs} files=${evt.filesProcessed}`));
  });

  const featureEngine = new FeatureEngine(eventBus);
  const klineFeatureEngine = new KlineFeatureEngine(eventBus);
  const flowEngine = new FlowEngine(eventBus);
  const liquidityEngine = new LiquidityEngine(eventBus);
  const marketContextBuilder = new MarketContextBuilder(eventBus);
  const marketViewBuilder = new MarketViewBuilder(eventBus);
  const regimeEngine = new RegimeEngineV1(eventBus);
  const patternMiner = new PatternMiner(eventBus);
  const strategyEngine = new StrategyEngine(eventBus);
  const riskManager = new RiskManager(eventBus);
  const execution = new PaperExecutionEngine(eventBus);
  const portfolio = new PortfolioManager(eventBus);
  const snapshotCoordinator = new SnapshotCoordinator(eventBus, {
    portfolio,
    risk: riskManager,
    strategy: strategyEngine,
  });
  const metricsCollector = new BacktestMetricsCollector(eventBus);
  const globalWeights = parseWeights(process.env.BOT_GLOBAL_WEIGHTS);
  const globalTtl = getGlobalTtlPolicy();
  const globalDataGateway = new GlobalDataGateway([], eventBus);
  const oiAggregator = new OpenInterestAggregator(eventBus, { ttlMs: globalTtl.oiMs, weights: globalWeights });
  const fundingAggregator = new FundingAggregator(eventBus, { ttlMs: globalTtl.fundingMs, weights: globalWeights });
  const cvdCalculator = new CvdCalculator(eventBus, { bucketMs: getCvdBucketMs() });
  const cvdAggregator = new CvdAggregator(eventBus, { ttlMs: globalTtl.cvdMs, weights: globalWeights });
  const liquidationAggregator = new LiquidationAggregator(eventBus, {
    ttlMs: globalTtl.liquidationsMs,
    bucketMs: getLiqBucketMs(),
    weights: globalWeights,
  });
  const liquidityAggregator = new LiquidityAggregator(eventBus, {
    ttlMs: globalTtl.liquidityMs,
    depthLevels: getLiquidityDepthLevels(),
    bucketMs: getLiqBucketMs(),
    weights: globalWeights,
  });
  const priceIndexAggregator = new PriceIndexAggregator(eventBus, { ttlMs: globalTtl.priceMs, weights: globalWeights });
  const canonicalPriceAggregator = new CanonicalPriceAggregator(eventBus, { ttlMs: globalTtl.priceMs, weights: globalWeights });
  const globalDataQuality = new GlobalDataQualityMonitor(eventBus, {
    expectedIntervalMs: {
      'market:oi_agg': 60_000,
      'market:funding_agg': 60_000,
      'market:cvd_spot_agg': getCvdBucketMs(),
      'market:cvd_futures_agg': getCvdBucketMs(),
      'market:liquidations_agg': getLiqBucketMs(),
      'market:liquidity_agg': getLiqBucketMs(),
      'market:price_index': 60_000,
      'market:price_canonical': 60_000,
    },
    mismatchWindowMs: 120_000,
  });
  const marketDataReadiness = new MarketDataReadiness(eventBus, {
    bucketMs: getReadinessBucketMs(),
    warmingWindowMs: getReadinessWarmupMs(),
    expectedSources: getExpectedSources(),
    logIntervalMs: getReadinessLogMs(),
  });
  featureEngine.start();
  klineFeatureEngine.start();
  flowEngine.start();
  liquidityEngine.start();
  marketContextBuilder.start();
  marketViewBuilder.start();
  regimeEngine.start();
  patternMiner.start();
  strategyEngine.start();
  riskManager.start();
  execution.start();
  portfolio.start();
  snapshotCoordinator.start();
  metricsCollector.start();
  globalDataGateway.start();
  oiAggregator.start();
  fundingAggregator.start();
  cvdCalculator.start();
  cvdAggregator.start();
  liquidationAggregator.start();
  liquidityAggregator.start();
  priceIndexAggregator.start();
  canonicalPriceAggregator.start();
  globalDataQuality.start();
  marketDataReadiness.start();

  eventBus.subscribe('metrics:backtest_summary', (evt) => {
    logger.info(m('ok', `[Backtest] runId=${evt.runId} fills=${evt.totalFills} pnl=${evt.realizedPnl.toFixed(2)} maxDD=${evt.maxDrawdown.toFixed(2)} start=${evt.startTs} end=${evt.endTs}`));
  });
  eventBus.subscribe('metrics:walkforward_summary', (evt) => {
    logger.info(
      m(
        'ok',
        `[WalkForward] runId=${evt.runId} folds=${evt.folds} trainMs=${evt.trainWindowMs} testMs=${evt.testWindowMs} stepMs=${evt.stepMs} start=${evt.startTs} end=${evt.endTs}`
      )
    );
  });

  try {
    if (args.walkForward) {
      if (!args.dateFrom || !args.dateTo) {
        throw new Error('walkforward requires --from and --to');
      }
      const wf = new WalkForwardRunner(eventBus, {
        baseDir: process.env.BOT_JOURNAL_DIR,
        streamId: args.streamId,
        symbol: args.symbol,
        runId: args.runId,
        dateFrom: args.dateFrom,
        dateTo: args.dateTo,
        trainDays: args.trainDays,
        testDays: args.testDays,
        stepDays: args.stepDays,
        mode: args.mode,
        speedFactor: args.speedFactor,
      });
      await wf.run();
    } else {
      await runner.run();
    }
  } finally {
    marketDataReadiness.stop();
    globalDataQuality.stop();
    priceIndexAggregator.stop();
    canonicalPriceAggregator.stop();
    liquidityAggregator.stop();
    liquidationAggregator.stop();
    cvdAggregator.stop();
    cvdCalculator.stop();
    oiAggregator.stop();
    fundingAggregator.stop();
    globalDataGateway.stop();
    metricsCollector.stop();
    snapshotCoordinator.stop();
    portfolio.stop();
    execution.stop();
    riskManager.stop();
    strategyEngine.stop();
    patternMiner.stop();
    regimeEngine.stop();
    marketViewBuilder.stop();
    marketContextBuilder.stop();
    liquidityEngine.stop();
    flowEngine.stop();
    klineFeatureEngine.stop();
    featureEngine.stop();
    tap.stop();
  }
}

main().catch((err) => {
  logger.error(m('error', `[Replay] fatal: ${(err as Error).message}`), err);
  process.exitCode = 1;
});
