import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsFeaturesEvent,
    type EventBus,
    type EventMeta,
    type MarketDataStatusPayload,
    type MarketContextEvent,
    type StrategyIntentEvent,
    type StrategyStateEvent,
    type StrategySide,
} from '../core/events/EventBus';

export interface StrategyEngineConfig {
    momentumThreshold?: number;
    minIntentIntervalMs?: number;
    targetExposureUsd?: number;
    decisionTraceIntervalMs?: number;
    marketNotReadyLogMs?: number;
}

interface SymbolState {
    lastIntentAt?: number;
    lastContext?: MarketContextEvent;
    rejectStats?: Map<StrategyRejectReason, RejectReasonStats>;
    lastRejectLogTs?: number;
    lastRejectKey?: string;
}

export interface StrategyStateSnapshot {
    lastIntentAt: Record<string, number>;
}

export class StrategyEngine {
    private readonly bus: EventBus;
    private readonly config: Required<StrategyEngineConfig>;
    private started = false;
    private unsubscribeFeatures?: () => void;
    private unsubscribeContext?: () => void;
    private unsubscribeMarketStatus?: () => void;
    private readonly state = new Map<string, SymbolState>();
    private marketReady = false;
    private lastMarketStatus?: MarketDataStatusPayload;
    private lastMarketNotReadyKey?: string;
    private readonly marketNotReadyLogs = new Map<string, { lastTs: number; suppressed: number }>();

    constructor(bus: EventBus = defaultEventBus, config: StrategyEngineConfig = {}) {
        this.bus = bus;
        this.config = {
            momentumThreshold: config.momentumThreshold ?? 0.001,
            minIntentIntervalMs: config.minIntentIntervalMs ?? 5_000,
            targetExposureUsd: config.targetExposureUsd ?? 25,
            decisionTraceIntervalMs: config.decisionTraceIntervalMs ?? 10_000,
            marketNotReadyLogMs: config.marketNotReadyLogMs ?? 5_000,
        };
    }

    start(): void {
        if (this.started) return;
        const onFeatures = (evt: AnalyticsFeaturesEvent) => this.handleFeatures(evt);
        const onContext = (evt: MarketContextEvent) => this.handleContext(evt);
        const onMarketStatus = (evt: MarketDataStatusPayload) => this.handleMarketStatus(evt);
        this.bus.subscribe('analytics:features', onFeatures);
        this.bus.subscribe('analytics:context', onContext);
        this.bus.subscribe('system:market_data_status', onMarketStatus);
        this.unsubscribeFeatures = () => this.bus.unsubscribe('analytics:features', onFeatures);
        this.unsubscribeContext = () => this.bus.unsubscribe('analytics:context', onContext);
        this.unsubscribeMarketStatus = () => this.bus.unsubscribe('system:market_data_status', onMarketStatus);
        this.started = true;
        logger.info(m('lifecycle', '[StrategyEngine] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribeFeatures?.();
        this.unsubscribeContext?.();
        this.unsubscribeMarketStatus?.();
        this.unsubscribeFeatures = undefined;
        this.unsubscribeContext = undefined;
        this.unsubscribeMarketStatus = undefined;
        this.started = false;
    }

    private handleMarketStatus(evt: MarketDataStatusPayload): void {
        this.lastMarketStatus = evt;
        this.marketReady = !evt.warmingUp && !evt.degraded;
    }

    private handleContext(evt: MarketContextEvent): void {
        const state = this.ensureState(evt.symbol);
        state.lastContext = evt;
    }

    private handleFeatures(evt: AnalyticsFeaturesEvent): void {
        if (this.lastMarketStatus && !this.marketReady) {
            this.maybeLogMarketNotReady(evt.meta.ts);
            return;
        }
        const state = this.ensureState(evt.symbol);
        const context = state.lastContext;
        if (!context) {
            this.recordReject(state, evt.meta.ts, ['contextMissing'], evt);
            return;
        }
        if (!context.contextReady) {
            this.recordReject(state, evt.meta.ts, ['contextNotReady'], evt);
            return;
        }
        if (context.regime !== 'calm') {
            this.recordReject(state, evt.meta.ts, ['regimeNotCalm'], evt);
            return;
        }
        if (!evt.featuresReady) {
            this.recordReject(state, evt.meta.ts, ['featuresNotReady'], evt);
            return;
        }
        if (evt.momentum === undefined) {
            this.recordReject(state, evt.meta.ts, ['momentumMissing'], evt);
            return;
        }

        const nowTs = evt.meta.ts;
        if (state.lastIntentAt !== undefined && nowTs - state.lastIntentAt < this.config.minIntentIntervalMs) {
            this.recordReject(state, nowTs, ['cooldown'], evt);
            this.publishState(evt.meta, evt.symbol, true);
            return;
        }

        const side: StrategySide | undefined = evt.momentum > this.config.momentumThreshold ? 'LONG' : undefined;
        if (!side) {
            this.recordReject(state, nowTs, ['momentumBelowThreshold'], evt);
            return;
        }

        const intent: StrategyIntentEvent = {
            intentId: `${evt.symbol}:${evt.meta.ts}`,
            symbol: evt.symbol,
            side,
            targetExposureUsd: this.config.targetExposureUsd,
            reason: 'momentum_long',
            constraints: { validityMs: 5_000 },
            ts: evt.meta.ts,
            meta: this.buildMeta(evt.meta),
        };

        this.bus.publish('strategy:intent', intent);
        state.lastIntentAt = nowTs;
        this.publishState(evt.meta, evt.symbol, false, state.lastIntentAt);
    }

    private recordReject(
        state: SymbolState,
        ts: number,
        reasons: StrategyRejectReason[],
        evt: AnalyticsFeaturesEvent
    ): void {
        if (!reasons.length) return;
        if (!state.rejectStats) state.rejectStats = new Map();
        for (const reason of reasons) {
            const existing = state.rejectStats.get(reason);
            if (existing) {
                existing.count += 1;
                existing.lastTs = ts;
            } else {
                state.rejectStats.set(reason, { count: 1, lastTs: ts });
            }
        }
        this.maybeLogRejectSummary(state, ts, reasons, evt);
    }

    private maybeLogRejectSummary(
        state: SymbolState,
        ts: number,
        reasons: StrategyRejectReason[],
        evt: AnalyticsFeaturesEvent
    ): void {
        const intervalMs = Math.max(0, this.config.decisionTraceIntervalMs);
        const key = reasons.slice().sort().join('|');
        const lastLogTs = state.lastRejectLogTs ?? 0;
        const shouldLogByTime = intervalMs === 0 || ts - lastLogTs >= intervalMs;
        const shouldLogByChange = key !== state.lastRejectKey;
        if (!shouldLogByTime && !shouldLogByChange) return;

        const stats = reasons
            .map((reason) => {
                const entry = state.rejectStats?.get(reason);
                const count = entry?.count ?? 0;
                const lastTs = entry?.lastTs ?? ts;
                return `${reason}=${count}@${lastTs}`;
            })
            .join(',');

        const corr = evt.meta.correlationId ?? 'n/a';
        const msg = `[Strategy] reject summary symbol=${evt.symbol} ts=${ts} corrId=${corr} reasons=${key} stats=${stats}`;
        logger.info(m('decision', msg));

        state.lastRejectLogTs = ts;
        state.lastRejectKey = key;
    }

    private maybeLogMarketNotReady(ts: number): void {
        const status = this.lastMarketStatus;
        const reasonList = status?.degradedReasons?.length ? [...status.degradedReasons].sort() : [];
        const reasons = reasonList.length ? reasonList.join(',') : 'n/a';
        const mode = status?.warmingUp ? 'WARMING' : status?.degraded ? 'DEGRADED' : 'READY';
        const key = `${mode}:${reasons}`;
        const intervalMs = Math.max(0, this.config.marketNotReadyLogMs);
        const entry = this.marketNotReadyLogs.get(key) ?? { lastTs: 0, suppressed: 0 };
        const shouldLogByChange = key !== this.lastMarketNotReadyKey;
        const shouldLogByTime = intervalMs === 0 || ts - entry.lastTs >= intervalMs;
        if (!shouldLogByChange && !shouldLogByTime) {
            entry.suppressed += 1;
            this.marketNotReadyLogs.set(key, entry);
            return;
        }

        const suppressedPart = entry.suppressed > 0 ? ` suppressedCount=${entry.suppressed}` : '';
        logger.debug(m('decision', `[Strategy] skip: market data ${mode} reasons=${reasons}${suppressedPart}`));
        entry.lastTs = ts;
        entry.suppressed = 0;
        this.marketNotReadyLogs.set(key, entry);
        this.lastMarketNotReadyKey = key;
    }

    private publishState(parent: EventMeta, symbol: string, throttled: boolean, lastIntentAt?: number): void {
        const payload: StrategyStateEvent = {
            symbol,
            throttled,
            lastIntentAt,
            ts: parent.ts,
            meta: this.buildMeta(parent),
        };
        this.bus.publish('strategy:state', payload);
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'strategy', { tsEvent: parent.tsEvent ?? parent.ts });
    }

    private ensureState(symbol: string): SymbolState {
        const existing = this.state.get(symbol);
        if (existing) return existing;
        const fresh: SymbolState = {};
        this.state.set(symbol, fresh);
        return fresh;
    }

    getState(): StrategyStateSnapshot {
        const lastIntentAt: Record<string, number> = {};
        for (const [symbol, st] of this.state.entries()) {
            if (st.lastIntentAt !== undefined) lastIntentAt[symbol] = st.lastIntentAt;
        }
        return { lastIntentAt };
    }

    setState(state: StrategyStateSnapshot): void {
        this.state.clear();
        const map = state?.lastIntentAt ?? {};
        for (const [symbol, ts] of Object.entries(map)) {
            this.state.set(symbol, { lastIntentAt: ts });
        }
    }
}

type StrategyRejectReason =
    | 'contextMissing'
    | 'contextNotReady'
    | 'regimeNotCalm'
    | 'featuresNotReady'
    | 'momentumMissing'
    | 'cooldown'
    | 'momentumBelowThreshold';

interface RejectReasonStats {
    count: number;
    lastTs: number;
}
