import { execSync } from 'child_process';
import { randomBytes } from 'crypto';
import fs from 'fs';
import path from 'path';
import { logger, type LogLevel } from '../infra/logger';
import { BybitPublicWsClient } from '../exchange/bybit/wsClient';
import { getBinancePublicWsClient } from '../exchange/binance/wsClient';
import { getOkxPublicWsClient } from '../exchange/okx/wsClient';
import { Orchestrator } from '../control/orchestrator';
import {
    createMeta,
    eventBus,
    type ControlCommand,
    type ControlState,
    type DataSourceDegraded,
    type DataSourceRecovered,
    type EventSource,
    type MarketConnected,
    type MarketDataStatusPayload,
    type MarketDisconnected,
    type RiskRejectedIntentEvent,
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
import { EventTap, type EventTapCounters } from '../observability/EventTap';
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
import { FileSink } from '../observability/logger/FileSink';
import { RotatingFileWriter } from '../observability/logger/RotatingFileWriter';
import { HealthReporter } from '../observability/health/HealthReporter';
import { DEFAULT_STALENESS_POLICY_RULES, type StalenessPolicyRule } from '../core/observability/stalenessPolicy';
import { DebouncedConsoleTransitions } from '../observability/ui/DebouncedConsoleTransitions';
import { formatDataSourceDegradedUiLine, formatDataSourceRecoveredUiLine } from '../observability/ui/dataQualityUi';

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

function getLogDir(): string {
    const raw = process.env.LOG_DIR?.trim();
    return raw && raw.length > 0 ? raw : 'logs';
}

function getLogLevel(): LogLevel {
    const raw = process.env.LOG_LEVEL?.trim().toLowerCase();
    if (raw === 'debug' || raw === 'info' || raw === 'warn' || raw === 'error') return raw;
    return 'info';
}

function getLogRotateMaxBytes(): number {
    return Math.max(0, Math.floor(parseNumber(process.env.LOG_ROTATE_MAX_BYTES, 10_485_760)));
}

function getLogRotateMaxFiles(): number {
    return Math.max(0, Math.floor(parseNumber(process.env.LOG_ROTATE_MAX_FILES, 5)));
}

function getHealthSnapshotIntervalMs(): number {
    return parseIntervalMs(process.env.HEALTH_SNAPSHOT_INTERVAL_MS, 5_000);
}

function buildDataQualityStalenessPolicy(): StalenessPolicyRule[] {
    const rules = DEFAULT_STALENESS_POLICY_RULES.map((rule) => ({ ...rule }));

    const oiRule = rules.find((rule) => rule.topic === 'market:oi_agg');
    if (oiRule) {
        oiRule.expectedIntervalMs = parseIntervalMs(process.env.BOT_DQ_OI_AGG_EXPECTED_INTERVAL_MS, oiRule.expectedIntervalMs);
        oiRule.staleThresholdMs = parseIntervalMs(process.env.BOT_DQ_OI_AGG_STALE_THRESHOLD_MS, oiRule.staleThresholdMs);
        oiRule.staleThresholdMs = Math.max(oiRule.expectedIntervalMs, oiRule.staleThresholdMs);
    }

    const fundingRule = rules.find((rule) => rule.topic === 'market:funding_agg');
    if (fundingRule) {
        fundingRule.expectedIntervalMs = parseIntervalMs(
            process.env.BOT_DQ_FUNDING_AGG_EXPECTED_INTERVAL_MS,
            fundingRule.expectedIntervalMs
        );
        fundingRule.staleThresholdMs = parseIntervalMs(
            process.env.BOT_DQ_FUNDING_AGG_STALE_THRESHOLD_MS,
            fundingRule.staleThresholdMs
        );
        fundingRule.staleThresholdMs = Math.max(fundingRule.expectedIntervalMs, fundingRule.staleThresholdMs);
    }

    return rules;
}

function getConsoleMode(): 'ui' | 'verbose' {
    const raw = process.env.CONSOLE_MODE?.trim().toLowerCase();
    if (raw === 'verbose') return 'verbose';
    return 'ui';
}

function getConsoleHeartbeatMs(): number {
    return parseIntervalMs(process.env.CONSOLE_HEARTBEAT_MS, 30_000);
}

function getConsoleTransitionCooldownMs(): number {
    return parseIntervalMs(process.env.CONSOLE_TRANSITION_COOLDOWN_MS, 10_000);
}

function getConsoleShowTicker(): boolean {
    return readFlagFromEnv('CONSOLE_SHOW_TICKER', false);
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
    const isDev = process.env.NODE_ENV !== 'production' || process.argv.includes('--dev');
    if (isDev) {
        return parseIntervalMs(process.env.BOT_READINESS_WARMUP_MS_DEV, 15_000);
    }
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

function getReadinessStabilityWindowMs(): number {
    return parseIntervalMs(process.env.BOT_READINESS_STABILITY_WINDOW_MS, 10_000);
}

function getReadinessLagWindowMs(): number {
    return parseIntervalMs(process.env.BOT_READINESS_LAG_WINDOW_MS, 30_000);
}

function getReadinessLagThresholdMs(): number {
    return parseIntervalMs(process.env.BOT_READINESS_LAG_THRESHOLD_MS, 2_000);
}

function getReadinessLagEwmaAlpha(): number {
    return parseNumber(process.env.BOT_READINESS_LAG_EWMA_ALPHA, 0.2);
}

function getReadinessTimebasePenaltyWindowMs(): number {
    return parseIntervalMs(process.env.BOT_READINESS_TIMEBASE_PENALTY_WINDOW_MS, 10_000);
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

function createRunId(): string {
    const d = new Date();
    const pad = (v: number) => String(v).padStart(2, '0');
    const ts = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(
        d.getUTCHours()
    )}-${pad(d.getUTCMinutes())}-${pad(d.getUTCSeconds())}Z`;
    let suffix = '000000';
    try {
        suffix = randomBytes(3).toString('hex');
    } catch {
        suffix = Math.floor(Math.random() * 0xffffff)
            .toString(16)
            .padStart(6, '0');
    }
    return `${ts}_${suffix}`;
}

function getBuildInfo(): BuildInfo {
    return {
        appVersion: getAppVersion(),
        gitVersion: getGitDescribe(),
        nodeVersion: process.version,
        pid: process.pid,
    };
}

function setupFileLogging(runId: string): void {
    const logDir = getLogDir();
    const maxBytes = getLogRotateMaxBytes();
    const maxFiles = getLogRotateMaxFiles();

    const touch = (filePath: string) => {
        try {
            fs.mkdirSync(path.dirname(filePath), { recursive: true });
            const fd = fs.openSync(filePath, 'a');
            fs.closeSync(fd);
        } catch {
            // ignore
        }
    };

    touch(path.join(logDir, 'app.log'));
    touch(path.join(logDir, 'warnings.log'));
    touch(path.join(logDir, 'errors.log'));
    touch(path.join(logDir, 'health.jsonl'));

    const appWriter = new RotatingFileWriter(path.join(logDir, 'app.log'), { maxBytes, maxFiles });
    const warningsWriter = new RotatingFileWriter(path.join(logDir, 'warnings.log'), { maxBytes, maxFiles });
    const errorsWriter = new RotatingFileWriter(path.join(logDir, 'errors.log'), { maxBytes, maxFiles });

    logger.addSink(new FileSink(appWriter, { runId }));
    // warnings.log: WARN + ERROR (documented choice)
    logger.addSink(new FileSink(warningsWriter, { runId, levels: ['warn', 'error'] }));
    logger.addSink(new FileSink(errorsWriter, { runId, levels: ['error'] }));
}

async function flushLogger(): Promise<void> {
    try {
        const anyLogger = logger as any;
        if (typeof anyLogger.flush === 'function') {
            await Promise.resolve(anyLogger.flush());
        }
        if (typeof anyLogger.close === 'function') {
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
    private uiHeartbeatInterval?: NodeJS.Timeout;
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
    private healthReporter?: HealthReporter;

    private readonly consoleHeartbeatMs = getConsoleHeartbeatMs();
    private readonly consoleTransitionCooldownMs = getConsoleTransitionCooldownMs();
    private readonly consoleShowTicker = getConsoleShowTicker();
    private readonly dataQualityConsoleGate = new DebouncedConsoleTransitions({
        debounceMs: this.consoleTransitionCooldownMs,
        holdDownMs: this.consoleTransitionCooldownMs,
    });
    private readonly dataQualityKeysBySourceId = new Map<string, string[]>();

    private lastMarketUiSignature?: string;
    private lastMarketUiTs?: number;
    private riskRejectCounts: Record<string, number> = {};
    private riskRejectTimer?: NodeJS.Timeout;
    private lastTapCounters?: EventTapCounters;

    private connectedCount = 0;
    private totalConnections = 0;

    private readonly enableCli = process.env.BOT_CLI !== '0';

    // handlers, чтобы на shutdown корректно отписаться
    private onTickerHandler?: (t: TickerEvent) => void;
    private onControlStateHandler?: (state: ControlState) => void;
    private onMarketStatusHandler?: (payload: MarketDataStatusPayload) => void;
    private onMarketConnectedHandler?: (evt: MarketConnected) => void;
    private onMarketDisconnectedHandler?: (evt: MarketDisconnected) => void;
    private onRiskRejectedHandler?: (evt: RiskRejectedIntentEvent) => void;
    private onDataSourceDegradedHandler?: (evt: DataSourceDegraded) => void;
    private onDataSourceRecoveredHandler?: (evt: DataSourceRecovered) => void;

    private controlState?: ControlState;
    private stopRequested = false;
    private stopped = false;
    private exitCode = 0;
    private stopTimeout?: NodeJS.Timeout;

    public async start(): Promise<void> {
        logger.infoFile(m('lifecycle', `${APP_TAG} Запущен торговый бот (live mode)...`));

        const build = getBuildInfo();
        logger.infoFile(
            m('ok', `${APP_TAG} Build info: appVersion=${build.appVersion} git=${build.gitVersion} node=${build.nodeVersion} pid=${build.pid}`)
        );

        const runId = logger.getRunId() ?? 'unknown';
        const logDir = getLogDir();
        logger.ui(`${APP_TAG} Старт: режим=${this.mode} runId=${runId}`);
        logger.ui(`${APP_TAG} Версия: app=${build.appVersion} git=${build.gitVersion} node=${build.nodeVersion}`);
        logger.ui(`${APP_TAG} Логи: ${logDir}/app.log, ${logDir}/warnings.log, ${logDir}/errors.log, ${logDir}/health.jsonl`);

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
        const binanceWs = getBinancePublicWsClient({
            url: 'wss://fstream.binance.com/ws',
            streamId: 'binance.usdm.public',
            marketType: 'futures',
            restClient: binanceRestClient,
            supportsLiquidations: true,
        });
        logger.info(
            m(
                'connect',
                `[MarketGateway] creating BinanceWS client id=${binanceWs.clientInstanceId} venue=binance streamId=binance.usdm.public symbol=n/a marketType=futures`
            )
        );
        const okxWs = getOkxPublicWsClient({
            url: 'wss://ws.okx.com:8443/ws/v5/public',
            streamId: 'okx.public.swap',
            marketType: 'futures',
            supportsLiquidations: true,
        });
        logger.info(
            m(
                'connect',
                `[MarketGateway] creating OKXWS client id=${okxWs.clientInstanceId} venue=okx streamId=${okxWs.streamId} symbol=n/a marketType=${okxWs.marketType ?? 'unknown'}`
            )
        );
        const okxKlineWs = okxEnableKlines
            ? getOkxPublicWsClient({
                  url: 'wss://ws.okx.com:8443/ws/v5/business',
                  streamId: 'okx.public.swap.kline',
                  marketType: 'futures',
                  supportsLiquidations: false,
              })
            : undefined;
        if (okxKlineWs) {
            logger.info(
                m(
                    'connect',
                    `[MarketGateway] creating OKXWS client id=${okxKlineWs.clientInstanceId} venue=okx streamId=${okxKlineWs.streamId} symbol=n/a marketType=${okxKlineWs.marketType ?? 'unknown'}`
                )
            );
        }

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
                topicFilter: (topic: string) => !topic.startsWith('kline.'),
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
                    topicFilter: (topic: string) => topic.startsWith('kline.'),
                })
            );
        }

        if (enableSpot) {
            const binanceSpotWs = getBinancePublicWsClient({
                url: 'wss://stream.binance.com:9443/ws',
                streamId: 'binance.spot.public',
                marketType: 'spot',
                supportsLiquidations: false,
                supportsOrderbook: false,
            });
            logger.info(
                m(
                    'connect',
                    `[MarketGateway] creating BinanceWS client id=${binanceSpotWs.clientInstanceId} venue=binance streamId=binance.spot.public symbol=n/a marketType=spot`
                )
            );
            const okxSpotWs = getOkxPublicWsClient({
                url: 'wss://ws.okx.com:8443/ws/v5/public',
                streamId: 'okx.public.spot',
                marketType: 'spot',
                supportsLiquidations: false,
            });
            logger.info(
                m(
                    'connect',
                    `[MarketGateway] creating OKXWS client id=${okxSpotWs.clientInstanceId} venue=okx streamId=${okxSpotWs.streamId} symbol=n/a marketType=${okxSpotWs.marketType ?? 'unknown'}`
                )
            );
            const okxSpotKlineWs = okxEnableKlines
                ? getOkxPublicWsClient({
                      url: 'wss://ws.okx.com:8443/ws/v5/business',
                      streamId: 'okx.public.spot.kline',
                      marketType: 'spot',
                      supportsLiquidations: false,
                  })
                : undefined;
            if (okxSpotKlineWs) {
                logger.info(
                    m(
                        'connect',
                        `[MarketGateway] creating OKXWS client id=${okxSpotKlineWs.clientInstanceId} venue=okx streamId=${okxSpotKlineWs.streamId} symbol=n/a marketType=${okxSpotKlineWs.marketType ?? 'unknown'}`
                    )
                );
            }
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
                    topicFilter: (topic: string) => !topic.startsWith('kline.'),
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
                        topicFilter: (topic: string) => topic.startsWith('kline.'),
                    })
                );
            }
        }

        this.totalConnections = this.marketGateways.length;
        logger.ui(`${APP_TAG} Подключения WS: ${this.connectedCount}/${this.totalConnections}`);

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
                'market:cvd_spot_agg': getCvdBucketMs(),
                'market:cvd_futures_agg': getCvdBucketMs(),
                'market:liquidations_agg': getLiqBucketMs(),
                'market:liquidity_agg': getLiqBucketMs(),
                'market:price_index': 60_000,
                'market:price_canonical': 60_000,
            },
            stalenessPolicy: buildDataQualityStalenessPolicy(),
            mismatchWindowMs: 120_000,
        });
        this.globalDataQuality.start();
        this.marketDataReadiness = new MarketDataReadiness(eventBus, {
            bucketMs: getReadinessBucketMs(),
            warmingWindowMs: getReadinessWarmupMs(),
            expectedSources: Math.max(1, getExpectedSources()),
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
            readinessStabilityWindowMs: getReadinessStabilityWindowMs(),
            lagWindowMs: getReadinessLagWindowMs(),
            lagThresholdMs: getReadinessLagThresholdMs(),
            lagEwmaAlpha: getReadinessLagEwmaAlpha(),
            timebasePenaltyWindowMs: getReadinessTimebasePenaltyWindowMs(),
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

        this.healthReporter = new HealthReporter({
            runId: logger.getRunId() ?? 'unknown',
            logDir: getLogDir(),
            intervalMs: getHealthSnapshotIntervalMs(),
            maxBytes: getLogRotateMaxBytes(),
            maxFiles: getLogRotateMaxFiles(),
            buildInfo: { appVersion: build.appVersion, gitVersion: build.gitVersion },
            eventTap: this.eventTap,
            marketDataReadiness: this.marketDataReadiness,
            globalDataQuality: this.globalDataQuality,
            getLifecycle: () => this.controlState?.lifecycle ?? 'STARTING',
            getMode: () => this.mode,
            getPaused: () => this.paused,
        });
        this.healthReporter.start();
        this.orchestrator.registerCleanup(() => this.healthReporter?.stop({ finalSnapshot: true }));

        this.orchestrator.start();

        // 2) Подписки на EventBus (пока временно: логирование и управление режимом)
        this.wireEventBusSubscriptions();

        // Запрашиваем актуальное состояние, чтобы синхронизироваться после подписки
        emitControlCommand({ type: 'status', source: 'system' });

        // 3) Подключение и подписка теперь делаются через orchestrator->market events (см. orchestrator.start())

        // 5) Console heartbeat (UI mode).
        if (this.consoleHeartbeatMs > 0) {
            this.uiHeartbeatInterval = setInterval(() => {
                if (this.stopRequested) return;
                const lifecycle = this.controlState?.lifecycle ?? 'n/a';
                const market = this.formatMarketUi();
                const flow = this.formatFlowUi();
                logger.ui(`${APP_TAG} Пульс: режим=${this.mode} пауза=${this.paused ? 'ДА' : 'НЕТ'} состояние=${lifecycle} ${market} ${flow}`);
            }, this.consoleHeartbeatMs);
            this.uiHeartbeatInterval.unref?.();
        }

        // 6) Консольное управление (CLI): команды идут в EventBus как control:command
        if (this.enableCli) {
            startCli({ logDir: getLogDir(), getStatusLines: () => this.buildStatusLines() });
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
            const line = `${APP_TAG} Ticker: ${symbol} last=${lastPrice} 24h=${p24h}`;
            if (this.consoleShowTicker && logger.getConsoleMode() === 'verbose') {
                logger.infoThrottled(`ticker:${symbol}`, line, 3000);
            } else {
                logger.infoFile(line);
            }
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

            if (state.lifecycle === 'STOPPING') {
                this.healthReporter?.snapshot();
            }

            if (state.lifecycle === 'STOPPED') {
                logger.info(m('shutdown', `${APP_TAG} STOPPED получено от Orchestrator — завершаем процесс`));
                void this.completeStop();
            }
        };
        eventBus.subscribe('control:state', this.onControlStateHandler);

        this.onMarketStatusHandler = (payload: MarketDataStatusPayload) => {
            this.handleMarketStatus(payload);
        };
        eventBus.subscribe('system:market_data_status', this.onMarketStatusHandler);

        this.onMarketConnectedHandler = (evt: MarketConnected) => {
            this.connectedCount = Math.min(this.totalConnections, this.connectedCount + 1);
            const url = evt.url ? ` url=${evt.url}` : '';
            logger.ui(`${APP_TAG} Подключение: +1 (${this.connectedCount}/${this.totalConnections})${url}`);
        };
        eventBus.subscribe('market:connected', this.onMarketConnectedHandler);

        this.onMarketDisconnectedHandler = (evt: MarketDisconnected) => {
            this.connectedCount = Math.max(0, this.connectedCount - 1);
            const reason = evt.reason ? ` причина=${evt.reason}` : '';
            logger.ui(`${APP_TAG} Подключение: -1 (${this.connectedCount}/${this.totalConnections})${reason}`);
        };
        eventBus.subscribe('market:disconnected', this.onMarketDisconnectedHandler);

        this.onRiskRejectedHandler = (evt: RiskRejectedIntentEvent) => {
            this.recordRiskReject(evt);
        };
        eventBus.subscribe('risk:rejected_intent', this.onRiskRejectedHandler);

        this.onDataSourceDegradedHandler = (evt: DataSourceDegraded) => {
            this.handleDataSourceDegraded(evt);
        };
        eventBus.subscribe('data:sourceDegraded', this.onDataSourceDegradedHandler);

        this.onDataSourceRecoveredHandler = (evt: DataSourceRecovered) => {
            this.handleDataSourceRecovered(evt);
        };
        eventBus.subscribe('data:sourceRecovered', this.onDataSourceRecoveredHandler);
    }

    private handleMarketStatus(payload: MarketDataStatusPayload): void {
        const status = payload.warmingUp ? 'WARMING' : payload.degraded ? 'DEGRADED' : 'READY';
        const reasons = payload.degradedReasons?.join('|') ?? '';
        const warnings = payload.warnings?.join('|') ?? '';
        const signature = `${status}|${reasons}|${warnings}`;
        const nowTs = Date.now();
        if (this.lastMarketUiSignature === signature) return;
        const lastTs = this.lastMarketUiTs ?? 0;
        if (nowTs - lastTs < this.consoleTransitionCooldownMs) return;
        this.lastMarketUiSignature = signature;
        this.lastMarketUiTs = nowTs;

        const statusRu = status === 'READY' ? 'ГОТОВ' : status === 'WARMING' ? 'ПРОГРЕВ' : 'ДЕГРАД';
        const conf = payload.overallConfidence.toFixed(2);
        const reasonPart = reasons ? ` причины=${reasons}` : '';
        const warnPart = warnings ? ` предупреждения=${warnings}` : '';
        logger.ui(`${APP_TAG} MarketData: ${statusRu} conf=${conf}${reasonPart}${warnPart}`);
    }

    private handleDataSourceDegraded(evt: DataSourceDegraded): void {
        const keys = this.resolveDataQualityKeys(evt.sourceId);
        const keysPart = keys.length ? ` keys=${keys.join(',')}` : '';
        logger.infoFile(
            m(
                'warn',
                `${APP_TAG} DataQuality: деградация sourceId=${evt.sourceId} reason=${evt.reason}${keysPart} lastSuccessTs=${evt.lastSuccessTs ?? 'n/a'}`
            )
        );

        if (!this.dataQualityConsoleGate.shouldEmitDegraded(evt.sourceId)) return;
        logger.uiConsole(
            formatDataSourceDegradedUiLine({
                appTag: APP_TAG,
                keys,
                sourceId: evt.sourceId,
                reason: evt.reason,
            })
        );
    }

    private handleDataSourceRecovered(evt: DataSourceRecovered): void {
        const keys = this.resolveDataQualityKeys(evt.sourceId);
        const keysPart = keys.length ? ` keys=${keys.join(',')}` : '';
        logger.infoFile(
            m(
                'ok',
                `${APP_TAG} DataQuality: восстановление sourceId=${evt.sourceId}${keysPart} recoveredTs=${evt.recoveredTs} lastErrorTs=${evt.lastErrorTs ?? 'n/a'}`
            )
        );

        if (!this.dataQualityConsoleGate.shouldEmitRecovered(evt.sourceId)) return;
        logger.uiConsole(
            formatDataSourceRecoveredUiLine({
                appTag: APP_TAG,
                keys,
                sourceId: evt.sourceId,
            })
        );
    }

    private resolveDataQualityKeys(sourceId: string, maxKeys = 3): string[] {
        const snapshot = this.globalDataQuality?.snapshot(50);
        const keys =
            snapshot?.degraded
                ?.filter((entry) => entry.sourceId === sourceId)
                .map((entry) => entry.key)
                .filter(Boolean) ?? [];

        if (keys.length) {
            this.dataQualityKeysBySourceId.set(sourceId, keys);
            return keys.slice(0, maxKeys);
        }

        const cached = this.dataQualityKeysBySourceId.get(sourceId);
        return cached ? cached.slice(0, maxKeys) : [];
    }

    private recordRiskReject(evt: RiskRejectedIntentEvent): void {
        const code = evt.reasonCode ?? 'UNKNOWN';
        this.riskRejectCounts[code] = (this.riskRejectCounts[code] ?? 0) + 1;
        if (!this.riskRejectTimer) {
            this.riskRejectTimer = setInterval(() => {
                this.flushRiskRejectSummary();
            }, 60_000);
            this.riskRejectTimer.unref?.();
        }
    }

    private flushRiskRejectSummary(force = false): void {
        const entries = Object.entries(this.riskRejectCounts).filter(([, count]) => count > 0);
        if (!entries.length && !force) return;
        if (entries.length) {
            const parts = entries.map(([code, count]) => `${code}=${count}`);
            logger.ui(`${APP_TAG} Отклонено сигналов: ${parts.join(', ')}`);
        }
        this.riskRejectCounts = {};
    }

    private formatMarketUi(): string {
        const snapshot = this.marketDataReadiness?.getHealthSnapshot();
        if (!snapshot) return 'рынок=н/д';
        const status = snapshot.status === 'READY' ? 'ГОТОВ' : snapshot.status === 'WARMING' ? 'ПРОГРЕВ' : 'ДЕГРАД';
        return `рынок=${status}`;
    }

    private formatFlowUi(): string {
        const counters = this.eventTap?.getCounters();
        if (!counters) return 'поток=н/д';
        const sum =
            counters.ticksSeen +
            counters.klinesSeen +
            counters.tradesSeen +
            counters.orderbookSnapshotsSeen +
            counters.orderbookDeltasSeen;
        const last = this.lastTapCounters;
        const lastSum = last
            ? last.ticksSeen +
              last.klinesSeen +
              last.tradesSeen +
              last.orderbookSnapshotsSeen +
              last.orderbookDeltasSeen
            : 0;
        this.lastTapCounters = counters;
        const status = sum > lastSum ? 'OK' : 'ПАУЗА';
        return `поток=${status}`;
    }

    private buildStatusLines(): string[] {
        const lines: string[] = [];
        const lifecycle = this.controlState?.lifecycle ?? 'n/a';
        const startedAt = this.controlState?.startedAt;
        const uptimeSec = startedAt ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000)) : 0;
        lines.push(`${APP_TAG} Статус: режим=${this.mode} пауза=${this.paused ? 'ДА' : 'НЕТ'} состояние=${lifecycle} аптайм=${uptimeSec}s`);

        const market = this.marketDataReadiness?.getHealthSnapshot();
        if (market) {
            const status = market.status === 'READY' ? 'ГОТОВ' : market.status === 'WARMING' ? 'ПРОГРЕВ' : 'ДЕГРАД';
            lines.push(`${APP_TAG} MarketData: ${market.marketType} ${status} conf=${market.conf.toFixed(2)}`);
        } else {
            lines.push(`${APP_TAG} MarketData: н/д`);
        }

        lines.push(`${APP_TAG} Подключения: ${this.connectedCount}/${this.totalConnections}`);
        lines.push(`${APP_TAG} Логи: ${getLogDir()}`);
        return lines;
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
            if (this.uiHeartbeatInterval) {
                clearInterval(this.uiHeartbeatInterval);
                this.uiHeartbeatInterval = undefined;
            }
        } catch {
            // ignore
        }

        try {
            this.healthReporter?.stop({ finalSnapshot: true });
            this.healthReporter = undefined;
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
            if (this.onMarketStatusHandler) {
                eventBus.unsubscribe('system:market_data_status', this.onMarketStatusHandler);
                this.onMarketStatusHandler = undefined;
            }
            if (this.onMarketConnectedHandler) {
                eventBus.unsubscribe('market:connected', this.onMarketConnectedHandler);
                this.onMarketConnectedHandler = undefined;
            }
            if (this.onMarketDisconnectedHandler) {
                eventBus.unsubscribe('market:disconnected', this.onMarketDisconnectedHandler);
                this.onMarketDisconnectedHandler = undefined;
            }
            if (this.onRiskRejectedHandler) {
                eventBus.unsubscribe('risk:rejected_intent', this.onRiskRejectedHandler);
                this.onRiskRejectedHandler = undefined;
            }
            if (this.onDataSourceDegradedHandler) {
                eventBus.unsubscribe('data:sourceDegraded', this.onDataSourceDegradedHandler);
                this.onDataSourceDegradedHandler = undefined;
            }
            if (this.onDataSourceRecoveredHandler) {
                eventBus.unsubscribe('data:sourceRecovered', this.onDataSourceRecoveredHandler);
                this.onDataSourceRecoveredHandler = undefined;
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

        try {
            this.flushRiskRejectSummary(true);
            if (this.riskRejectTimer) {
                clearInterval(this.riskRejectTimer);
                this.riskRejectTimer = undefined;
            }
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
    const runId = createRunId();
    logger.setRunId(runId);
    logger.setLevel(getLogLevel());
    logger.setConsoleMode(getConsoleMode());
    setupFileLogging(runId);

    const app = new LiveBotApp();

    const onFatal = async (label: string, err: unknown) => {
        logger.ui(`${APP_TAG} Ошибка: ${label}. Подробности в logs/errors.log`);
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
    logger.ui(`${APP_TAG} Ошибка: fatal. Подробности в logs/errors.log`);
    logger.error(`${APP_TAG} Fatal error in trading bot:`, error);

    await flushLogger();

    process.exitCode = 1;
    setTimeout(() => process.exit(1), 250);
});
