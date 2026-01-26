import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { logger } from '../infra/logger';
import { BybitPublicWsClient } from '../exchange/bybit/wsClient';
import { BinancePublicWsClient } from '../exchange/binance/wsClient';
import { OkxPublicWsClient } from '../exchange/okx/wsClient';
import { Orchestrator } from '../control/orchestrator';
import {
    createMeta,
    eventBus,
    type ControlCommand,
    type ControlState,
    type EventSource,
    type TickerEvent,
} from '../core/events/EventBus';
import { MarketGateway } from '../exchange/marketGateway';
import { BybitDerivativesRestPoller, BybitPublicRestClient } from '../exchange/bybit/restClient';
import { BinanceDerivativesRestPoller, BinancePublicRestClient } from '../exchange/binance/restClient';
import { OkxDerivativesRestPoller, OkxPublicRestClient } from '../exchange/okx/restClient';
import { CompositeDerivativesCollector } from '../exchange/CompositeDerivativesCollector';
import { startCli } from '../cli/cliApp';
import { m } from '../core/logMarkers';
import { createEventJournal, EventJournal } from '../storage/eventJournal';
import { createAggregatedEventJournal, type AggregatedEventJournal } from '../storage/aggregatedJournal';
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
import { EventTap } from '../observability/EventTap';
import { GlobalDataGateway } from '../globalData/GlobalDataGateway';
import { OpenInterestAggregator } from '../globalData/OpenInterestAggregator';
import { FundingAggregator } from '../globalData/FundingAggregator';
import { CvdCalculator } from '../globalData/CvdCalculator';
import { CvdAggregator } from '../globalData/CvdAggregator';
import { LiquidationAggregator } from '../globalData/LiquidationAggregator';
import { LiquidityAggregator } from '../globalData/LiquidityAggregator';
import { CanonicalPriceAggregator } from '../globalData/CanonicalPriceAggregator';
import { PriceIndexAggregator } from '../globalData/PriceIndexAggregator';
import { GlobalDataQualityMonitor } from '../globalData/GlobalDataQualityMonitor';
import { PatternMiner } from '../research/PatternMiner';
import { MarketDataReadiness } from '../observability/MarketDataReadiness';
import { parseExpectedSourcesConfig, type ExpectedSourcesConfig } from '../core/market/expectedSources';

const APP_NAME = 'vavanbot';
const APP_TAG = `[${APP_NAME}]`;

// Синхронный лог: печатается даже если логгер буферизует.
const logSync = (msg: string) => {
    try {
        process.stderr.write(msg + '\n');
    } catch {
        // ignore
    }
};

type BotMode = 'LIVE' | 'PAPER' | 'BACKTEST';

type BuildInfo = {
    appVersion: string;
    gitVersion: string;
    nodeVersion: string;
    pid: number;
};

function getAppVersion(): string {
    try {
        // live-bot.ts лежит в src/apps → ../../package.json = корень проекта
        const pkgPath = path.resolve(__dirname, '../../package.json');
        const raw = fs.readFileSync(pkgPath, 'utf8');
        const pkg = JSON.parse(raw);
        if (pkg?.version) return String(pkg.version);
    } catch {
        // ignore
    }

    // Иногда npm прокидывает версию через env
    if (process.env.npm_package_version) return String(process.env.npm_package_version);

    return 'unknown';
}

function getGitDescribe(): string {
    try {
        return execSync('git describe --tags --dirty --always', {
            stdio: ['ignore', 'pipe', 'ignore'],
        })
            .toString()
            .trim();
    } catch {
        // например, если запуск без .git (docker/prod)
        return 'n/a';
    }
}

function getOiIntervalTime(): string {
    const raw = process.env.BOT_OI_INTERVAL_TIME;
    return raw && raw.trim() ? raw.trim() : '5min';
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

function readFlagFromEnv(name: string, fallback = true): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
    if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
    return fallback;
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

function getReadinessStartupGraceMs(): number {
    return parseIntervalMs(process.env.BOT_READINESS_STARTUP_GRACE_MS, 10_000);
}

function getDerivativesStaleWindowMs(): number {
    return parseIntervalMs(process.env.BOT_DERIVATIVES_STALE_WINDOW_MS, 0);
}

function getTargetMarketType(): 'spot' | 'futures' | undefined {
    const raw = process.env.BOT_TARGET_MARKET_TYPE?.trim().toLowerCase();
    if (raw === 'spot' || raw === 'futures') return raw;
    return undefined;
}

function readSymbols(): string[] {
    const raw = process.env.BOT_SYMBOLS ?? process.env.BOT_SYMBOL ?? 'BTCUSDT';
    const list = raw
        .split(',')
        .map((entry) => entry.trim().toUpperCase())
        .filter(Boolean);
    return list.length ? list : ['BTCUSDT'];
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

function buildExpectedSourcesConfig(
    globalWeights: Record<string, number>,
    enableSpot: boolean
): ExpectedSourcesConfig | undefined {
    const parsed = parseExpectedSourcesConfig(process.env.BOT_EXPECTED_SOURCES_CONFIG);
    if (parsed) return parsed;

    const weightedSources = Object.keys(globalWeights);
    const futuresSources = weightedSources.length
        ? weightedSources
        : ['bybit.public.linear.v5', 'binance.usdm.public', 'okx.public.swap'];
    const spotSources = enableSpot ? ['binance.spot.public', 'okx.public.spot'] : [];

    return {
        default: {
            futures: {
                price: futuresSources,
                flow: futuresSources,
                liquidity: futuresSources,
                derivatives: futuresSources,
            },
            spot: {
                price: spotSources,
                flow: spotSources,
                liquidity: spotSources,
                derivatives: [],
            },
        },
    };
}

function getOkxOiIntervalMs(): number {
    return parseIntervalMs(process.env.BOT_OKX_OI_POLL_MS, 30_000);
}

function getOkxFundingIntervalMs(): number {
    return parseIntervalMs(process.env.BOT_OKX_FUNDING_POLL_MS, 60_000);
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

function getBuildInfo(): BuildInfo {
    return {
        appVersion: getAppVersion(),
        gitVersion: getGitDescribe(),
        nodeVersion: process.version,
        pid: process.pid,
    };
}

async function flushLogger(): Promise<void> {
    try {
        const anyLogger = logger as any;
        if (typeof anyLogger.flush === 'function') {
            await Promise.resolve(anyLogger.flush());
        } else if (typeof anyLogger.close === 'function') {
            await Promise.resolve(anyLogger.close());
        } else if (typeof anyLogger.end === 'function') {
            await Promise.resolve(anyLogger.end());
        }
    } catch {
        // ignore
    }
}

function emitControlCommand(cmd: { type: 'pause' | 'resume' | 'shutdown' | 'status'; source: EventSource; reason?: string }) {
    // Пока Control Plane не выделен в отдельный модуль, публикуем команды в EventBus.
    const controlCommand: ControlCommand = {
        type: cmd.type,
        reason: cmd.reason,
        meta: createMeta(cmd.source),
    } as ControlCommand;

    eventBus.publish('control:command', controlCommand);
}

class LiveBotApp {
    private mode: BotMode = 'LIVE';
    private paused = false;

    private marketGateways: MarketGateway[] = [];
    private healthInterval?: NodeJS.Timeout;
    private orchestrator?: Orchestrator;
    private journal?: EventJournal;
    private aggregatedJournal?: AggregatedEventJournal;
    private featureEngine?: FeatureEngine;
    private klineFeatureEngine?: KlineFeatureEngine;
    private flowEngine?: FlowEngine;
    private liquidityEngine?: LiquidityEngine;
    private marketContextBuilder?: MarketContextBuilder;
    private marketViewBuilder?: MarketViewBuilder;
    private regimeEngine?: RegimeEngineV1;
    private patternMiner?: PatternMiner;
    private strategyEngine?: StrategyEngine;
    private riskManager?: RiskManager;
    private execution?: PaperExecutionEngine;
    private portfolio?: PortfolioManager;
    private snapshotCoordinator?: SnapshotCoordinator;
    private eventTap?: EventTap;
    private globalDataGateway?: GlobalDataGateway;
    private globalDataQuality?: GlobalDataQualityMonitor;
    private oiAggregator?: OpenInterestAggregator;
    private fundingAggregator?: FundingAggregator;
    private cvdCalculator?: CvdCalculator;
    private cvdAggregator?: CvdAggregator;
    private liquidationAggregator?: LiquidationAggregator;
    private liquidityAggregator?: LiquidityAggregator;
    private priceIndexAggregator?: PriceIndexAggregator;
    private canonicalPriceAggregator?: CanonicalPriceAggregator;
    private marketDataReadiness?: MarketDataReadiness;

    private readonly enableCli = process.env.BOT_CLI !== '0';

    // handlers, чтобы на shutdown корректно отписаться
    private onTickerHandler?: (t: TickerEvent) => void;
    private onControlStateHandler?: (state: ControlState) => void;

    private controlState?: ControlState;
    private stopRequested = false;
    private stopped = false;
    private exitCode = 0;
    private stopTimeout?: NodeJS.Timeout;

    public async start(): Promise<void> {
        logger.info(m('lifecycle', `${APP_TAG} Запущен торговый бот (live mode)...`));

        const build = getBuildInfo();
        logger.info(
            m('ok', `${APP_TAG} Build info: appVersion=${build.appVersion} git=${build.gitVersion} node=${build.nodeVersion} pid=${build.pid}`)
        );

        // 1) Поднимаем Market Data Plane (multi-exchange WS + REST)
        const globalWeights = parseWeights(process.env.BOT_GLOBAL_WEIGHTS);
        const readinessTargetMarketType = getTargetMarketType();
        const enableSpot = readinessTargetMarketType ? readinessTargetMarketType === 'spot' : process.env.BOT_SPOT_ENABLED !== '0';
        const expectedSourcesConfig = buildExpectedSourcesConfig(globalWeights, enableSpot);
        const enableOi = readFlagFromEnv('BOT_OI_ENABLED', true);
        const enableFunding = readFlagFromEnv('BOT_FUNDING_ENABLED', true);
        const enableLiquidations = readFlagFromEnv('BOT_LIQUIDATIONS_ENABLED', false);
        const okxEnableKlines = readFlagFromEnv('OKX_ENABLE_KLINES', true);
        const expectedDerivativeKinds = [
            ...(enableOi ? (['oi'] as const) : []),
            ...(enableFunding ? (['funding'] as const) : []),
            ...(enableLiquidations ? (['liquidations'] as const) : []),
        ];
        const spotFlowExpected = Boolean(expectedSourcesConfig?.default?.spot?.flow?.length);
        const expectedFlowTypes = spotFlowExpected ? (['spot', 'futures'] as const) : (['futures'] as const);

        const bybitRestClient = new BybitPublicRestClient();
        const bybitPoller = new BybitDerivativesRestPoller(bybitRestClient, eventBus, {
            oiIntervalTime: getOiIntervalTime(),
        });

        const binanceRestClient = new BinancePublicRestClient();
        const binancePoller = new BinanceDerivativesRestPoller(binanceRestClient, eventBus, {});

        const okxRestClient = new OkxPublicRestClient();
        const okxPoller = new OkxDerivativesRestPoller(okxRestClient, eventBus, {
            oiIntervalMs: getOkxOiIntervalMs(),
            fundingIntervalMs: getOkxFundingIntervalMs(),
        });

        const readinessConfidenceWindowMs = Math.max(
            getCvdBucketMs(),
            getLiqBucketMs(),
            getOkxOiIntervalMs(),
            getOkxFundingIntervalMs()
        );
        const derivativesStaleWindowMs = Math.max(
            getDerivativesStaleWindowMs(),
            getOkxOiIntervalMs(),
            getOkxFundingIntervalMs()
        );

        const derivativesPoller = new CompositeDerivativesCollector([bybitPoller, binancePoller, okxPoller]);

        const bybitWs = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear', {
            streamId: 'bybit.public.linear.v5',
            marketType: 'futures',
            supportsLiquidations: true,
        });
        const binanceWs = new BinancePublicWsClient('wss://fstream.binance.com/ws', {
            streamId: 'binance.usdm.public',
            marketType: 'futures',
            restClient: binanceRestClient,
            supportsLiquidations: true,
        });
        const okxWs = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
            streamId: 'okx.public.swap',
            marketType: 'futures',
            supportsLiquidations: true,
        });
        const okxKlineWs = okxEnableKlines
            ? new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/business', {
                  streamId: 'okx.public.swap.kline',
                  marketType: 'futures',
                  supportsLiquidations: false,
              })
            : undefined;

        this.marketGateways = [
            new MarketGateway(bybitWs, bybitRestClient, eventBus, derivativesPoller, { venue: 'bybit', marketType: 'futures' }),
            new MarketGateway(binanceWs, binanceRestClient, eventBus, derivativesPoller, {
                venue: 'binance',
                marketType: 'futures',
                resyncStrategy: 'ignore',
            }),
            new MarketGateway(okxWs, okxRestClient, eventBus, derivativesPoller, {
                venue: 'okx',
                marketType: 'futures',
                topicFilter: (topic) => !topic.startsWith('kline.'),
            }),
        ];
        if (okxKlineWs) {
            this.marketGateways.push(
                new MarketGateway(okxKlineWs, okxRestClient, eventBus, derivativesPoller, {
                    venue: 'okx',
                    marketType: 'futures',
                    enableKlineBootstrap: false,
                    enableResync: false,
                    resyncStrategy: 'ignore',
                    topicFilter: (topic) => topic.startsWith('kline.'),
                })
            );
        }

        if (enableSpot) {
            const binanceSpotWs = new BinancePublicWsClient('wss://stream.binance.com:9443/ws', {
                streamId: 'binance.spot.public',
                marketType: 'spot',
                supportsLiquidations: false,
                supportsOrderbook: false,
            });
            const okxSpotWs = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
                streamId: 'okx.public.spot',
                marketType: 'spot',
                supportsLiquidations: false,
            });
            const okxSpotKlineWs = okxEnableKlines
                ? new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/business', {
                      streamId: 'okx.public.spot.kline',
                      marketType: 'spot',
                      supportsLiquidations: false,
                  })
                : undefined;
            this.marketGateways.push(
                new MarketGateway(binanceSpotWs, binanceRestClient, eventBus, derivativesPoller, {
                    enableKlineBootstrap: false,
                    enableResync: false,
                    venue: 'binance',
                    marketType: 'spot',
                    resyncStrategy: 'ignore',
                }),
                new MarketGateway(okxSpotWs, okxRestClient, eventBus, derivativesPoller, {
                    enableKlineBootstrap: false,
                    enableResync: false,
                    venue: 'okx',
                    marketType: 'spot',
                    topicFilter: (topic) => !topic.startsWith('kline.'),
                })
            );
            if (okxSpotKlineWs) {
                this.marketGateways.push(
                    new MarketGateway(okxSpotKlineWs, okxRestClient, eventBus, derivativesPoller, {
                        enableKlineBootstrap: false,
                        enableResync: false,
                        resyncStrategy: 'ignore',
                        venue: 'okx',
                        marketType: 'spot',
                        topicFilter: (topic) => topic.startsWith('kline.'),
                    })
                );
            }
        }

        // 1.5) Поднимаем Control Plane (Orchestrator), регистрируем cleanup
        this.orchestrator = new Orchestrator();
        this.orchestrator.registerCleanup(async () => {
            await Promise.all(this.marketGateways.map((gateway) => gateway.shutdown('orchestrator cleanup')));
        });
        this.orchestrator.registerCleanup(async () => {
            if (this.journal) await this.journal.stop();
            if (this.aggregatedJournal) await this.aggregatedJournal.stop();
        });

        this.featureEngine = new FeatureEngine(eventBus);
        this.klineFeatureEngine = new KlineFeatureEngine(eventBus);
        this.flowEngine = new FlowEngine(eventBus);
        this.liquidityEngine = new LiquidityEngine(eventBus);
        this.marketContextBuilder = new MarketContextBuilder(eventBus);
        this.marketViewBuilder = new MarketViewBuilder(eventBus);
        this.regimeEngine = new RegimeEngineV1(eventBus);
        this.patternMiner = new PatternMiner(eventBus);
        this.strategyEngine = new StrategyEngine(eventBus);
        this.riskManager = new RiskManager(eventBus);
        this.execution = new PaperExecutionEngine(eventBus);
        this.portfolio = new PortfolioManager(eventBus);
        this.featureEngine.start();
        this.klineFeatureEngine.start();
        this.flowEngine.start();
        this.liquidityEngine.start();
        this.marketContextBuilder.start();
        this.marketViewBuilder.start();
        this.regimeEngine.start();
        this.patternMiner.start();
        this.strategyEngine.start();
        this.riskManager.start();
        this.execution.start();
        this.portfolio.start();
        this.globalDataGateway = new GlobalDataGateway([], eventBus);
        this.globalDataGateway.start();
        const globalTtl = getGlobalTtlPolicy();
        this.oiAggregator = new OpenInterestAggregator(eventBus, { ttlMs: globalTtl.oiMs, weights: globalWeights });
        this.oiAggregator.start();
        this.fundingAggregator = new FundingAggregator(eventBus, { ttlMs: globalTtl.fundingMs, weights: globalWeights });
        this.fundingAggregator.start();
        this.cvdCalculator = new CvdCalculator(eventBus, { bucketMs: getCvdBucketMs() });
        this.cvdCalculator.start();
        this.cvdAggregator = new CvdAggregator(eventBus, { ttlMs: globalTtl.cvdMs, weights: globalWeights });
        this.cvdAggregator.start();
        this.liquidationAggregator = new LiquidationAggregator(eventBus, {
            ttlMs: globalTtl.liquidationsMs,
            bucketMs: getLiqBucketMs(),
            weights: globalWeights,
        });
        this.liquidationAggregator.start();
        this.liquidityAggregator = new LiquidityAggregator(eventBus, {
            ttlMs: globalTtl.liquidityMs,
            depthLevels: getLiquidityDepthLevels(),
            bucketMs: getLiqBucketMs(),
            weights: globalWeights,
        });
        this.liquidityAggregator.start();
        this.priceIndexAggregator = new PriceIndexAggregator(eventBus, { ttlMs: globalTtl.priceMs, weights: globalWeights });
        this.priceIndexAggregator.start();
        this.canonicalPriceAggregator = new CanonicalPriceAggregator(eventBus, { ttlMs: globalTtl.priceMs, weights: globalWeights });
        this.canonicalPriceAggregator.start();
        this.globalDataQuality = new GlobalDataQualityMonitor(eventBus, {
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
        this.globalDataQuality.start();
        this.marketDataReadiness = new MarketDataReadiness(eventBus, {
            bucketMs: getReadinessBucketMs(),
            warmingWindowMs: getReadinessWarmupMs(),
            expectedSources: getExpectedSources(),
            expectedSourcesConfig,
            expectedSourcesByBlock: {
                price: Object.keys(globalWeights),
                flow: Object.keys(globalWeights),
                liquidity: Object.keys(globalWeights),
                derivatives: Object.keys(globalWeights),
            },
            expectedFlowTypes: [...expectedFlowTypes],
            expectedDerivativeKinds: [...expectedDerivativeKinds],
            logIntervalMs: getReadinessLogMs(),
            confidenceStaleWindowMs: readinessConfidenceWindowMs,
            derivativesStaleWindowMs,
            targetMarketType: getTargetMarketType(),
            startupGraceWindowMs: getReadinessStartupGraceMs(),
        });
        const targetMarketType = readinessTargetMarketType;
        const symbols = readSymbols();
        const marketTypesToSeed = targetMarketType
            ? [targetMarketType]
            : enableSpot
              ? (['futures', 'spot'] as const)
              : (['futures'] as const);
        for (const symbol of symbols) {
            for (const marketType of marketTypesToSeed) {
                this.marketDataReadiness.seedExpectedSources({ symbol, marketType });
            }
        }
        this.marketDataReadiness.start();

        for (const gateway of this.marketGateways) {
            gateway.start();
        }
        this.snapshotCoordinator = new SnapshotCoordinator(eventBus, {
            portfolio: this.portfolio,
            risk: this.riskManager,
            strategy: this.strategyEngine,
        });
        this.snapshotCoordinator.start();
        this.orchestrator.registerCleanup(() => this.featureEngine?.stop());
        this.orchestrator.registerCleanup(() => this.klineFeatureEngine?.stop());
        this.orchestrator.registerCleanup(() => this.flowEngine?.stop());
        this.orchestrator.registerCleanup(() => this.liquidityEngine?.stop());
        this.orchestrator.registerCleanup(() => this.marketContextBuilder?.stop());
        this.orchestrator.registerCleanup(() => this.marketViewBuilder?.stop());
        this.orchestrator.registerCleanup(() => this.regimeEngine?.stop());
        this.orchestrator.registerCleanup(() => this.patternMiner?.stop());
        this.orchestrator.registerCleanup(() => this.strategyEngine?.stop());
        this.orchestrator.registerCleanup(() => this.riskManager?.stop());
        this.orchestrator.registerCleanup(() => this.execution?.stop());
        this.orchestrator.registerCleanup(() => this.portfolio?.stop());
        this.orchestrator.registerCleanup(() => this.globalDataGateway?.stop());
        this.orchestrator.registerCleanup(() => this.oiAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.fundingAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.cvdCalculator?.stop());
        this.orchestrator.registerCleanup(() => this.cvdAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.liquidationAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.liquidityAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.priceIndexAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.canonicalPriceAggregator?.stop());
        this.orchestrator.registerCleanup(() => this.globalDataQuality?.stop());
        this.orchestrator.registerCleanup(() => this.marketDataReadiness?.stop());
        this.orchestrator.registerCleanup(() => this.snapshotCoordinator?.stop());

        // 1.1) Event Journal (Storage plane): пишет market:ticker в JSONL
        const baseDir = process.env.BOT_JOURNAL_DIR ?? './data/journal';
        this.journal = createEventJournal(eventBus, { baseDir });
        this.journal.start();

        // 1.2) Aggregated/System Journal: пишет system:market_data_status
        this.aggregatedJournal = createAggregatedEventJournal(eventBus, { baseDir });
        this.aggregatedJournal.start();

        // Observability: signal-only tap for key pipeline events
        this.eventTap = new EventTap(eventBus, { summaryIntervalMs: 10_000 });
        this.eventTap.start();
        this.orchestrator.registerCleanup(() => this.eventTap?.stop());

        this.orchestrator.start();

        // 2) Подписки на EventBus (пока временно: логирование и управление режимом)
        this.wireEventBusSubscriptions();

        // Запрашиваем актуальное состояние, чтобы синхронизироваться после подписки
        emitControlCommand({ type: 'status', source: 'system' });

        // 3) Подключение и подписка теперь делаются через orchestrator->market events (см. orchestrator.start())

        // 5) Health-пульс. Позже это станет частью Control Plane: health/status.
        this.healthInterval = setInterval(() => {
            if (this.stopRequested) return;
            logger.info(m('lifecycle', `${APP_TAG} Status: mode=${this.mode} paused=${this.paused} lifecycle=${this.controlState?.lifecycle ?? 'n/a'} (bot alive)`));
            // TODO(ControlPlane): publish control:state from Orchestrator
        }, 30_000);

        // 6) Консольное управление (CLI): команды идут в EventBus как control:command
        if (this.enableCli) {
            startCli();
        } else {
            logger.info(`${APP_TAG} CLI disabled (BOT_CLI=0)`);
        }
    }

    private wireEventBusSubscriptions(): void {
        // market:ticker → выводим с троттлингом, чтобы не спамить консоль
        this.onTickerHandler = (t: TickerEvent) => {
            const symbol = t?.symbol ?? 'n/a';
            const lastPrice = t?.lastPrice ?? 'n/a';
            const p24h = t?.price24hPcnt ?? 'n/a';

            logger.infoThrottled(`ticker:${symbol}`, `${APP_TAG} Ticker: ${symbol} last=${lastPrice} 24h=${p24h}`, 3000);
        };
        eventBus.subscribe('market:ticker', this.onTickerHandler);

        // storage:writeFailed → логируем единично на path, чтобы не спамить
        const loggedStorageErrors = new Set<string>();
        eventBus.subscribe('storage:writeFailed', (evt) => {
            const key = `${evt.path}`;
            if (loggedStorageErrors.has(key)) return;
            loggedStorageErrors.add(key);
            logger.error(m('error', `${APP_TAG} journal write failed for ${evt.path}: ${(evt.error as Error)?.message ?? evt.error}`));
        });

        this.onControlStateHandler = (state: ControlState) => {
            this.controlState = state;
            this.mode = state.mode;
            this.paused = state.paused;

            if (state.lifecycle === 'STOPPED') {
                logger.info(m('shutdown', `${APP_TAG} STOPPED получено от Orchestrator — завершаем процесс`));
                void this.completeStop();
            }
        };
        eventBus.subscribe('control:state', this.onControlStateHandler);
    }

    public requestShutdown(reason: string, exitCode = 0): void {
        if (this.stopRequested) return;
        this.stopRequested = true;
        this.exitCode = exitCode;

        logSync(`${APP_TAG} shutdown requested: ${reason}`);
        emitControlCommand({ type: 'shutdown', source: 'system', reason });

        // Фолбэк: если STOPPED не придёт, завершимся сами.
        this.stopTimeout = setTimeout(() => {
            logger.warn(`${APP_TAG} Shutdown timeout, forcing exit`);
            void this.completeStop(true);
        }, 5_000);
        this.stopTimeout.unref?.();
    }

    private async completeStop(force = false): Promise<void> {
        if (this.stopped) return;
        this.stopped = true;

        try {
            if (this.healthInterval) {
                clearInterval(this.healthInterval);
                this.healthInterval = undefined;
            }
        } catch {
            // ignore
        }

        try {
            if (this.onTickerHandler) {
                eventBus.unsubscribe('market:ticker', this.onTickerHandler);
                this.onTickerHandler = undefined;
            }
            if (this.onControlStateHandler) {
                eventBus.unsubscribe('control:state', this.onControlStateHandler);
                this.onControlStateHandler = undefined;
            }
        } catch {
            // ignore
        }

        try {
            if (this.journal) {
                await this.journal.stop();
            }
            if (this.aggregatedJournal) {
                await this.aggregatedJournal.stop();
            }
            this.featureEngine?.stop();
            this.klineFeatureEngine?.stop();
            this.flowEngine?.stop();
            this.liquidityEngine?.stop();
            this.marketContextBuilder?.stop();
            this.strategyEngine?.stop();
            this.riskManager?.stop();
            this.execution?.stop();
            this.portfolio?.stop();
            this.snapshotCoordinator?.stop();
        } catch {
            // ignore
        }

        try {
            this.orchestrator?.stop();
        } catch {
            // ignore
        }

        if (force && this.marketGateways.length) {
            await Promise.all(
                this.marketGateways.map(async (gateway) => {
                    try {
                        await gateway.shutdown('forced shutdown');
                    } catch {
                        // ignore
                    }
                })
            );
        }

        if (this.stopTimeout) {
            clearTimeout(this.stopTimeout);
            this.stopTimeout = undefined;
        }

        await flushLogger();

        process.exitCode = this.exitCode;
        logSync(`${APP_TAG} shutdown complete, exitCode=${process.exitCode ?? 0}`);
        process.exit(process.exitCode ?? 0);
    }
}

async function main(): Promise<void> {
    const app = new LiveBotApp();

    const onFatal = async (label: string, err: unknown) => {
        logger.error(`${APP_TAG} ${label}:`, err);
        app.requestShutdown(label, 1);
    };

    process.on('unhandledRejection', (reason) => void onFatal('unhandledRejection', reason));
    process.on('uncaughtException', (err) => void onFatal('uncaughtException', err));

    // SIGINT/SIGTERM: публикуем команду, а shutdown делаем через app.
    process.once('SIGINT', () => {
        logSync(`${APP_TAG} SIGINT received`);
        app.requestShutdown('SIGINT', 0);
    });
    process.once('SIGTERM', () => {
        logSync(`${APP_TAG} SIGTERM received`);
        app.requestShutdown('SIGTERM', 0);
    });

    await app.start();
}

main().catch(async (error) => {
    try {
        process.stderr.write(`${APP_TAG} Fatal error in trading bot: ${String(error)}\n`);
    } catch {
        // ignore
    }
    logger.error(`${APP_TAG} Fatal error in trading bot:`, error);

    await flushLogger();

    process.exitCode = 1;
    setTimeout(() => process.exit(1), 250);
});
