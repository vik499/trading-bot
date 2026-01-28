import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsFeaturesEvent,
    type AnalyticsReadyEvent,
    type EventBus,
    type EventMeta,
    type MarketContextEvent,
    type MarketRegime,
    type MarketRegimeV2,
} from '../core/events/EventBus';

export interface MarketContextConfig {
    lowVolThreshold?: number;
    highVolThreshold?: number;
    contextWarmup?: number;
    macroTfs?: string[];
}

interface ContextState {
    processedCount: number;
    contextReady: boolean;
    lastEmaSlow?: number;
    emaSlowSlope?: number;
    lastFeatures?: AnalyticsFeaturesEvent;
}

export interface MarketContextStateSnapshot {
    symbols: Record<string, ContextState>;
}

export class MarketContextBuilder {
    private readonly bus: EventBus;
    private readonly config: Required<MarketContextConfig>;
    private started = false;
    private unsubscribe?: () => void;
    private readyUnsubscribe?: () => void;
    private readonly state = new Map<string, ContextState>();
    private readonly readyKeys = new Set<string>();
    private readonly readyBySymbol = new Map<string, Set<string>>();
    private readonly macroReadySymbols = new Set<string>();
    private readonly macroReadyEmitted = new Set<string>();

    constructor(bus: EventBus = defaultEventBus, config: MarketContextConfig = {}) {
        this.bus = bus;
        this.config = {
            lowVolThreshold: config.lowVolThreshold ?? 0.0008,
            highVolThreshold: config.highVolThreshold ?? 0.0016,
            contextWarmup: Math.max(1, config.contextWarmup ?? 5),
            macroTfs: config.macroTfs ?? ['1h', '4h', '1d'],
        };
    }

    start(): void {
        if (this.started) return;
        const handler = (evt: AnalyticsFeaturesEvent) => this.onFeatures(evt);
        const readyHandler = (evt: AnalyticsReadyEvent) => this.onReady(evt);
        this.bus.subscribe('analytics:features', handler);
        this.bus.subscribe('analytics:ready', readyHandler);
        this.unsubscribe = () => this.bus.unsubscribe('analytics:features', handler);
        this.readyUnsubscribe = () => this.bus.unsubscribe('analytics:ready', readyHandler);
        this.started = true;
        logger.info(m('lifecycle', '[MarketContextBuilder] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribe?.();
        this.readyUnsubscribe?.();
        this.unsubscribe = undefined;
        this.readyUnsubscribe = undefined;
        this.started = false;
    }

    private onReady(evt: AnalyticsReadyEvent): void {
        if (!evt.ready) return;
        if (evt.reason === 'macroWarmup') return;
        const tf = evt.tf ?? 'tick';
        const key = this.makeKey(evt.symbol, tf);
        this.readyKeys.add(key);
        if (evt.reason === 'klineWarmup') {
            const set = this.readyBySymbol.get(evt.symbol) ?? new Set<string>();
            set.add(tf);
            this.readyBySymbol.set(evt.symbol, set);
            if (this.isMacroReady(evt.symbol) && !this.macroReadyEmitted.has(evt.symbol)) {
                this.macroReadyEmitted.add(evt.symbol);
                this.macroReadySymbols.add(evt.symbol);
                const readyEvent: AnalyticsReadyEvent = {
                    symbol: evt.symbol,
                    ts: evt.meta.ts,
                    tf: 'macro',
                    ready: true,
                    reason: 'macroWarmup',
                    readyTfs: [...set].sort(),
                    meta: this.buildMeta(evt.meta),
                };
                this.bus.publish('analytics:ready', readyEvent);
            }
        }
    }

    private onFeatures(features: AnalyticsFeaturesEvent): void {
        const symbol = features.symbol;
        const tf = features.klineTf ?? 'tick';
        const key = this.makeKey(symbol, tf);
        const state = this.ensureState(key);
        state.processedCount += 1;

        const isKline = features.sourceTopic === 'market:kline';
        if (isKline && this.config.macroTfs.includes(tf)) {
            const prevEmaSlow = state.lastEmaSlow;
            if (features.emaSlow !== undefined) {
                state.emaSlowSlope = prevEmaSlow !== undefined ? features.emaSlow - prevEmaSlow : 0;
                state.lastEmaSlow = features.emaSlow;
            }
            state.lastFeatures = features;
        }

        if (this.macroReadySymbols.has(symbol)) {
            if (!isKline || !this.config.macroTfs.includes(tf)) return;
            const resolved = this.resolveMacroRegime(symbol);
            if (!resolved) return;
            const meta = this.buildMeta(features.meta);
            const payload: MarketContextEvent = {
                symbol,
                ts: features.meta.ts,
                regime: resolved.regime,
                regimeV2: resolved.regimeV2,
                volatility: resolved.volatility,
                atr: resolved.atr,
                atrPct: resolved.atrPct,
                featuresReady: true,
                contextReady: true,
                processedCount: state.processedCount,
                tf: 'macro',
                sourceTopic: 'market:kline',
                meta,
            };
            this.bus.publish('analytics:context', payload);
            return;
        }

        if (!this.readyKeys.has(key)) return;
        if (isKline) return;

        const resolved = this.resolveRegime(features, state, false);
        const contextReady = state.processedCount >= this.config.contextWarmup;
        state.contextReady = contextReady;

        const meta = this.buildMeta(features.meta);
        const payload: MarketContextEvent = {
            symbol,
            ts: features.meta.ts,
            regime: resolved.regime,
            regimeV2: resolved.regimeV2,
            volatility: resolved.volatility,
            atr: resolved.atr,
            atrPct: resolved.atrPct,
            featuresReady: features.featuresReady,
            contextReady,
            processedCount: state.processedCount,
            tf,
            sourceTopic: features.sourceTopic,
            meta,
        };

        this.bus.publish('analytics:context', payload);
    }

    private resolveRegime(
        features: AnalyticsFeaturesEvent,
        state: ContextState,
        isKline: boolean
    ): {
        regime: MarketRegime;
        regimeV2?: MarketRegimeV2;
        volatility?: number;
        atr?: number;
        atrPct?: number;
    } {
        if (!features.featuresReady) {
            return { regime: 'unknown', volatility: features.volatility };
        }

        if (isKline && features.emaFast !== undefined && features.emaSlow !== undefined && features.atr !== undefined) {
            const prevEmaSlow = state.lastEmaSlow;
            state.lastEmaSlow = features.emaSlow;
            const slope = prevEmaSlow !== undefined ? features.emaSlow - prevEmaSlow : 0;
            const bullTrend = features.emaFast > features.emaSlow && slope > 0;
            const bearTrend = features.emaFast < features.emaSlow && slope < 0;
            const close = features.lastPrice;
            const atrPct = close !== 0 ? features.atr / close : undefined;

            if (atrPct !== undefined && atrPct >= this.config.highVolThreshold) {
                return {
                    regime: 'volatile',
                    regimeV2: 'storm',
                    volatility: atrPct,
                    atr: features.atr,
                    atrPct,
                };
            }

            if (bullTrend) {
                return {
                    regime: 'calm',
                    regimeV2: 'trend_bull',
                    volatility: atrPct,
                    atr: features.atr,
                    atrPct,
                };
            }

            if (bearTrend) {
                return {
                    regime: 'calm',
                    regimeV2: 'trend_bear',
                    volatility: atrPct,
                    atr: features.atr,
                    atrPct,
                };
            }

            return {
                regime: 'calm',
                regimeV2: 'calm_range',
                volatility: atrPct,
                atr: features.atr,
                atrPct,
            };
        }

        if (features.volatility === undefined) {
            return { regime: 'unknown' };
        }
        if (features.volatility < this.config.lowVolThreshold) return { regime: 'calm', volatility: features.volatility };
        if (features.volatility >= this.config.highVolThreshold) return { regime: 'volatile', volatility: features.volatility };
        return { regime: 'calm', volatility: features.volatility };
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'analytics', { tsEvent: parent.tsEvent ?? parent.ts });
    }

    private makeKey(symbol: string, tf: string): string {
        return `${symbol}:${tf}`;
    }

    private ensureState(key: string): ContextState {
        const existing = this.state.get(key);
        if (existing) return existing;
        const fresh: ContextState = { processedCount: 0, contextReady: false };
        this.state.set(key, fresh);
        return fresh;
    }

    private isMacroReady(symbol: string): boolean {
        const set = this.readyBySymbol.get(symbol);
        if (!set) return false;
        return this.config.macroTfs.every((tf) => set.has(tf));
    }

    private resolveMacroRegime(symbol: string): {
        regime: MarketRegime;
        regimeV2?: MarketRegimeV2;
        volatility?: number;
        atr?: number;
        atrPct?: number;
    } | null {
        let bullCount = 0;
        let bearCount = 0;
        let atrPctMax: number | undefined;
        let atrForMax: number | undefined;

        for (const tf of this.config.macroTfs) {
            const key = this.makeKey(symbol, tf);
            const state = this.state.get(key);
            const features = state?.lastFeatures;
            if (!features || features.emaFast === undefined || features.emaSlow === undefined || features.atr === undefined) {
                return null;
            }
            const slope = state.emaSlowSlope ?? 0;
            const bull = features.emaFast > features.emaSlow && slope > 0;
            const bear = features.emaFast < features.emaSlow && slope < 0;
            if (bull) bullCount += 1;
            if (bear) bearCount += 1;

            const close = features.lastPrice;
            const atrPct = close !== 0 ? features.atr / close : undefined;
            if (atrPct !== undefined && (atrPctMax === undefined || atrPct > atrPctMax)) {
                atrPctMax = atrPct;
                atrForMax = features.atr;
            }
        }

        if (atrPctMax !== undefined && atrPctMax >= this.config.highVolThreshold) {
            return {
                regime: 'volatile',
                regimeV2: 'storm',
                volatility: atrPctMax,
                atr: atrForMax,
                atrPct: atrPctMax,
            };
        }

        if (bullCount === this.config.macroTfs.length) {
            return {
                regime: 'calm',
                regimeV2: 'trend_bull',
                volatility: atrPctMax,
                atr: atrForMax,
                atrPct: atrPctMax,
            };
        }

        if (bearCount === this.config.macroTfs.length) {
            return {
                regime: 'calm',
                regimeV2: 'trend_bear',
                volatility: atrPctMax,
                atr: atrForMax,
                atrPct: atrPctMax,
            };
        }

        return {
            regime: 'calm',
            regimeV2: 'calm_range',
            volatility: atrPctMax,
            atr: atrForMax,
            atrPct: atrPctMax,
        };
    }

    getState(): MarketContextStateSnapshot {
        const symbols: Record<string, ContextState> = {};
        for (const [key, st] of this.state.entries()) {
            symbols[key] = { ...st };
        }
        return { symbols };
    }

    setState(state: MarketContextStateSnapshot): void {
        this.state.clear();
        const symbols = state?.symbols ?? {};
        for (const [key, st] of Object.entries(symbols)) {
            this.state.set(key, { ...st });
        }
    }
}
