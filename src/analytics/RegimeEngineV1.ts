import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsMarketViewEvent,
    type AnalyticsRegimeEvent,
    type AnalyticsRegimeExplainEvent,
    type EventBus,
    type EventMeta,
    type MarketRegime,
    type MarketRegimeV2,
} from '../core/events/EventBus';

export interface RegimeEngineConfig {
    highVolThreshold?: number;
    explainThrottleMs?: number;
}

interface RegimeState {
    lastEmaSlow?: number;
    lastExplainTs?: number;
    lastRegime?: MarketRegimeV2;
}

export class RegimeEngineV1 {
    private readonly bus: EventBus;
    private readonly config: Required<RegimeEngineConfig>;
    private started = false;
    private unsubscribe?: () => void;
    private readonly state = new Map<string, RegimeState>();

    constructor(bus: EventBus = defaultEventBus, config: RegimeEngineConfig = {}) {
        this.bus = bus;
        this.config = {
            highVolThreshold: config.highVolThreshold ?? 0.0016,
            explainThrottleMs: Math.max(0, config.explainThrottleMs ?? 5_000),
        };
    }

    start(): void {
        if (this.started) return;
        const handler = (evt: AnalyticsMarketViewEvent) => this.onMarketView(evt);
        this.bus.subscribe('analytics:market_view', handler);
        this.unsubscribe = () => this.bus.unsubscribe('analytics:market_view', handler);
        this.started = true;
        logger.info(m('lifecycle', '[RegimeEngineV1] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    private onMarketView(evt: AnalyticsMarketViewEvent): void {
        const symbol = evt.symbol;
        const state = this.ensureState(symbol);
        const macro = evt.macro ?? evt.micro;
        if (!macro) return;

        const emaSlow = macro.emaSlow;
        const emaFast = macro.emaFast;
        const close = macro.close;
        const atr = macro.atr;

        const prevEmaSlow = state.lastEmaSlow;
        const slope = prevEmaSlow !== undefined && emaSlow !== undefined ? emaSlow - prevEmaSlow : 0;
        if (emaSlow !== undefined) state.lastEmaSlow = emaSlow;

        const atrPct = close && atr !== undefined && close !== 0 ? atr / close : undefined;
        const bullTrend = emaFast !== undefined && emaSlow !== undefined && emaFast > emaSlow && slope > 0;
        const bearTrend = emaFast !== undefined && emaSlow !== undefined && emaFast < emaSlow && slope < 0;

        let regimeV2: MarketRegimeV2 = 'calm_range';
        let regime: MarketRegime = 'calm';

        if (atrPct !== undefined && atrPct >= this.config.highVolThreshold) {
            regimeV2 = 'storm';
            regime = 'volatile';
        } else if (bullTrend) {
            regimeV2 = 'trend_bull';
        } else if (bearTrend) {
            regimeV2 = 'trend_bear';
        }

        const meta = this.buildMeta(evt.meta);
        const regimeEvent: AnalyticsRegimeEvent = {
            symbol,
            ts: evt.meta.ts,
            regime,
            regimeV2,
            confidence: this.computeConfidence(atrPct, slope, emaSlow),
            sourceTopic: 'analytics:market_view',
            meta,
        };
        this.bus.publish('analytics:regime', regimeEvent);

        const shouldExplain = this.shouldEmitExplain(state, evt.meta.ts, regimeV2);
        if (!shouldExplain) return;

        const explain: AnalyticsRegimeExplainEvent = {
            symbol,
            ts: evt.meta.ts,
            regime,
            regimeV2,
            factors: {
                emaFast,
                emaSlow,
                slope,
                atr,
                atrPct,
                close,
                highVolThreshold: this.config.highVolThreshold,
            },
            sourceTopic: 'analytics:market_view',
            meta,
        };
        this.bus.publish('analytics:regime_explain', explain);
        state.lastExplainTs = evt.meta.ts;
        state.lastRegime = regimeV2;
    }

    private computeConfidence(atrPct: number | undefined, slope: number, emaSlow: number | undefined): number | undefined {
        if (atrPct === undefined || emaSlow === undefined) return undefined;
        const slopePct = emaSlow !== 0 ? Math.abs(slope / emaSlow) : 0;
        const raw = Math.min(1, (atrPct + slopePct) * 10);
        return Number.isFinite(raw) ? raw : undefined;
    }

    private shouldEmitExplain(state: RegimeState, ts: number, regime: MarketRegimeV2): boolean {
        const interval = this.config.explainThrottleMs;
        const lastTs = state.lastExplainTs ?? 0;
        if (state.lastRegime !== regime) return true;
        if (interval === 0) return true;
        return ts - lastTs >= interval;
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'analytics', { ts: parent.ts });
    }

    private ensureState(symbol: string): RegimeState {
        const existing = this.state.get(symbol);
        if (existing) return existing;
        const fresh: RegimeState = {};
        this.state.set(symbol, fresh);
        return fresh;
    }
}
