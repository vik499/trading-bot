import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsReadyEvent,
    type AnalyticsFeaturesEvent,
    type EventBus,
    type EventMeta,
    type KlineEvent,
    type TickerEvent,
} from '../core/events/EventBus';

export interface FeatureEngineConfig {
    windowSize?: number;
    smaPeriod?: number;
    minEmitIntervalMs?: number;
    maxTicksBeforeEmit?: number;
}

interface SymbolState {
    prices: number[];
    lastEmitTs?: number;
    ticksSinceEmit: number;
    featuresReady: boolean;
    readyEmitted: boolean;
}

export interface FeatureEngineState {
    symbols: Record<string, SymbolState>;
}

export class FeatureEngine {
    private readonly bus: EventBus;
    private readonly config: Required<FeatureEngineConfig>;
    private started = false;
    private unsubscribe?: () => void;
    private readonly state = new Map<string, SymbolState>();

    constructor(bus: EventBus = defaultEventBus, config: FeatureEngineConfig = {}) {
        const smaPeriod = Math.max(1, config.smaPeriod ?? 20);
        const windowSize = Math.max(smaPeriod, config.windowSize ?? 60);
        this.bus = bus;
        this.config = {
            windowSize,
            smaPeriod,
            minEmitIntervalMs: Math.max(0, config.minEmitIntervalMs ?? 1000),
            maxTicksBeforeEmit: Math.max(1, config.maxTicksBeforeEmit ?? 5),
        };
    }

    start(): void {
        if (this.started) return;
        const handler = (ticker: TickerEvent) => this.onTicker(ticker);
        this.bus.subscribe('market:ticker', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:ticker', handler);
        this.started = true;
        logger.info(m('lifecycle', '[FeatureEngine] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    private onTicker(ticker: TickerEvent): void {
        const price = this.extractPrice(ticker);
        if (price === undefined) return;
        const symbol = ticker.symbol ?? 'n/a';

        const state = this.ensureState(symbol);
        const prevPrice = state.prices[state.prices.length - 1];
        const return1 = prevPrice !== undefined && prevPrice !== 0 ? (price - prevPrice) / prevPrice : undefined;

        state.prices.push(price);
        if (state.prices.length > this.config.windowSize) {
            state.prices.shift();
        }

        const sampleCount = state.prices.length;
        const smaReady = sampleCount >= this.config.smaPeriod;
        const sma20 = smaReady ? this.mean(state.prices.slice(-this.config.smaPeriod)) : undefined;
        const volatility = smaReady ? this.computeVolatility(state.prices, this.config.smaPeriod) : undefined;
        const momentum = sma20 !== undefined && sma20 !== 0 ? (price - sma20) / sma20 : undefined;

        const featuresReady = smaReady;
        const readinessJustReached = featuresReady && !state.featuresReady;
        state.featuresReady = featuresReady;

        state.ticksSinceEmit += 1;
        const sinceLast = state.lastEmitTs !== undefined ? ticker.meta.ts - state.lastEmitTs : Number.POSITIVE_INFINITY;
        const shouldEmit =
            readinessJustReached ||
            state.lastEmitTs === undefined ||
            state.ticksSinceEmit >= this.config.maxTicksBeforeEmit ||
            sinceLast >= this.config.minEmitIntervalMs;

        if (!shouldEmit) return;

        const meta = this.buildMeta(ticker.meta);
        if (readinessJustReached && !state.readyEmitted) {
            const ready: AnalyticsReadyEvent = {
                symbol,
                ts: ticker.meta.ts,
                tf: 'tick',
                ready: true,
                reason: 'tickerWarmup',
                meta: this.buildMeta(ticker.meta),
            };
            this.bus.publish('analytics:ready', ready);
            state.readyEmitted = true;
        }

        const payload: AnalyticsFeaturesEvent = {
            symbol,
            ts: ticker.meta.ts,
            lastPrice: price,
            return1,
            sma20,
            volatility,
            momentum,
            sampleCount,
            featuresReady,
            windowSize: this.config.windowSize,
            smaPeriod: this.config.smaPeriod,
            meta,
            sourceTopic: 'market:ticker',
        };

        this.bus.publish('analytics:features', payload);
        state.lastEmitTs = ticker.meta.ts;
        state.ticksSinceEmit = 0;
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'analytics', { ts: parent.ts });
    }

    private ensureState(symbol: string): SymbolState {
        const existing = this.state.get(symbol);
        if (existing) return existing;
        const fresh: SymbolState = { prices: [], ticksSinceEmit: 0, featuresReady: false, readyEmitted: false };
        this.state.set(symbol, fresh);
        return fresh;
    }

    getState(): FeatureEngineState {
        const symbols: Record<string, SymbolState> = {};
        for (const [symbol, st] of this.state.entries()) {
            symbols[symbol] = {
                prices: [...st.prices.slice(-this.config.windowSize)],
                lastEmitTs: st.lastEmitTs,
                ticksSinceEmit: st.ticksSinceEmit,
                featuresReady: st.featuresReady,
                readyEmitted: st.readyEmitted,
            };
        }
        return { symbols };
    }

    setState(state: FeatureEngineState): void {
        this.state.clear();
        const symbols = state?.symbols ?? {};
        for (const [symbol, st] of Object.entries(symbols)) {
            this.state.set(symbol, {
                prices: [...(st.prices ?? [])].slice(-this.config.windowSize),
                lastEmitTs: st.lastEmitTs,
                ticksSinceEmit: st.ticksSinceEmit ?? 0,
                featuresReady: Boolean(st.featuresReady),
                readyEmitted: Boolean(st.readyEmitted ?? st.featuresReady),
            });
        }
    }

    private extractPrice(ticker: TickerEvent): number | undefined {
        const raw = ticker.lastPrice ?? ticker.markPrice ?? ticker.indexPrice;
        const price = raw !== undefined ? Number(raw) : undefined;
        if (price === undefined || !Number.isFinite(price)) return undefined;
        return price;
    }

    private mean(values: number[]): number {
        if (!values.length) return 0;
        return values.reduce((sum, v) => sum + v, 0) / values.length;
    }

    private computeVolatility(prices: number[], period: number): number | undefined {
        const slice = prices.slice(-(period + 1));
        if (slice.length < 2) return undefined;
        const returns: number[] = [];
        for (let i = 1; i < slice.length; i++) {
            const prev = slice[i - 1];
            const curr = slice[i];
            if (prev === 0) continue;
            returns.push((curr - prev) / prev);
        }
        if (returns.length < 2) return undefined;
        const mean = this.mean(returns);
        const variance = this.mean(returns.map((r) => (r - mean) ** 2));
        return Math.sqrt(variance);
    }
}

export interface KlineFeatureEngineConfig {
    emaFastPeriod?: number;
    emaSlowPeriod?: number;
    rsiPeriod?: number;
    atrPeriod?: number;
}

interface KlineState {
    count: number;
    emaFast?: number;
    emaSlow?: number;
    rsi?: number;
    rsiAvgGain?: number;
    rsiAvgLoss?: number;
    rsiSeedCount: number;
    rsiGainSum: number;
    rsiLossSum: number;
    atr?: number;
    atrSeedCount: number;
    atrSum: number;
    prevClose?: number;
    readyEmitted: boolean;
}

export class KlineFeatureEngine {
    private readonly bus: EventBus;
    private readonly config: Required<KlineFeatureEngineConfig>;
    private started = false;
    private unsubscribe?: () => void;
    private readonly state = new Map<string, KlineState>();

    constructor(bus: EventBus = defaultEventBus, config: KlineFeatureEngineConfig = {}) {
        this.bus = bus;
        this.config = {
            emaFastPeriod: Math.max(1, config.emaFastPeriod ?? 12),
            emaSlowPeriod: Math.max(1, config.emaSlowPeriod ?? 26),
            rsiPeriod: Math.max(1, config.rsiPeriod ?? 14),
            atrPeriod: Math.max(1, config.atrPeriod ?? 14),
        };
    }

    start(): void {
        if (this.started) return;
        const handler = (evt: KlineEvent) => this.onKline(evt);
        this.bus.subscribe('market:kline', handler);
        this.unsubscribe = () => this.bus.unsubscribe('market:kline', handler);
        this.started = true;
        logger.info(m('lifecycle', '[KlineFeatureEngine] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    private onKline(kline: KlineEvent): void {
        const symbol = kline.symbol ?? 'n/a';
        const tf = kline.tf;
        const key = `${symbol}:${tf}`;
        const state = this.ensureState(key);

        const close = kline.close;
        const prevClose = state.prevClose;

        state.count += 1;

        state.emaFast = this.updateEma(state.emaFast, close, this.config.emaFastPeriod);
        state.emaSlow = this.updateEma(state.emaSlow, close, this.config.emaSlowPeriod);

        if (prevClose !== undefined) {
            const delta = close - prevClose;
            const gain = delta > 0 ? delta : 0;
            const loss = delta < 0 ? -delta : 0;

            if (state.rsiAvgGain === undefined || state.rsiAvgLoss === undefined) {
                state.rsiGainSum += gain;
                state.rsiLossSum += loss;
                state.rsiSeedCount += 1;
                if (state.rsiSeedCount >= this.config.rsiPeriod) {
                    state.rsiAvgGain = state.rsiGainSum / this.config.rsiPeriod;
                    state.rsiAvgLoss = state.rsiLossSum / this.config.rsiPeriod;
                    state.rsi = this.computeRsi(state.rsiAvgGain, state.rsiAvgLoss);
                }
            } else {
                state.rsiAvgGain = (state.rsiAvgGain * (this.config.rsiPeriod - 1) + gain) / this.config.rsiPeriod;
                state.rsiAvgLoss = (state.rsiAvgLoss * (this.config.rsiPeriod - 1) + loss) / this.config.rsiPeriod;
                state.rsi = this.computeRsi(state.rsiAvgGain, state.rsiAvgLoss);
            }

            const tr = this.computeTrueRange(kline, prevClose);
            if (state.atr === undefined) {
                state.atrSum += tr;
                state.atrSeedCount += 1;
                if (state.atrSeedCount >= this.config.atrPeriod) {
                    state.atr = state.atrSum / this.config.atrPeriod;
                }
            } else {
                state.atr = (state.atr * (this.config.atrPeriod - 1) + tr) / this.config.atrPeriod;
            }
        }

        state.prevClose = close;

        const warmupNeeded = Math.max(this.config.emaSlowPeriod, this.config.rsiPeriod + 1, this.config.atrPeriod + 1);
        const indicatorsReady = state.emaSlow !== undefined && state.rsi !== undefined && state.atr !== undefined;
        const featuresReady = state.count >= warmupNeeded && indicatorsReady;
        const readinessJustReached = featuresReady && !state.readyEmitted;

        const meta = this.buildMeta(kline.meta);

        if (readinessJustReached) {
            const ready: AnalyticsReadyEvent = {
                symbol,
                ts: kline.meta.ts,
                tf,
                ready: true,
                reason: 'klineWarmup',
                meta,
            };
            this.bus.publish('analytics:ready', ready);
            state.readyEmitted = true;
        }

        const return1 =
            prevClose !== undefined && prevClose !== 0 ? (close - prevClose) / prevClose : undefined;

        const payload: AnalyticsFeaturesEvent = {
            symbol,
            ts: kline.meta.ts,
            closeTs: kline.endTs,
            lastPrice: close,
            return1,
            emaFast: state.emaFast,
            emaSlow: state.emaSlow,
            rsi: state.rsi,
            atr: state.atr,
            sampleCount: state.count,
            featuresReady,
            windowSize: warmupNeeded,
            meta,
            klineTf: tf,
            sourceTopic: 'market:kline',
        };

        this.bus.publish('analytics:features', payload);
    }

    private updateEma(prev: number | undefined, value: number, period: number): number {
        if (prev === undefined) return value;
        const alpha = 2 / (period + 1);
        return value * alpha + prev * (1 - alpha);
    }

    private computeRsi(avgGain: number, avgLoss: number): number {
        if (avgLoss === 0) return 100;
        const rs = avgGain / avgLoss;
        return 100 - 100 / (1 + rs);
    }

    private computeTrueRange(kline: KlineEvent, prevClose: number): number {
        const rangeHighLow = kline.high - kline.low;
        const rangeHighClose = Math.abs(kline.high - prevClose);
        const rangeLowClose = Math.abs(kline.low - prevClose);
        return Math.max(rangeHighLow, rangeHighClose, rangeLowClose);
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'analytics', { ts: parent.ts });
    }

    private ensureState(key: string): KlineState {
        const existing = this.state.get(key);
        if (existing) return existing;
        const fresh: KlineState = {
            count: 0,
            rsiSeedCount: 0,
            rsiGainSum: 0,
            rsiLossSum: 0,
            atrSeedCount: 0,
            atrSum: 0,
            readyEmitted: false,
        };
        this.state.set(key, fresh);
        return fresh;
    }
}
