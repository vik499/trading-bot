import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type ControlState,
    type EventBus,
    type EventMeta,
    type MarketContextEvent,
    type MarketDataStatusPayload,
    type RiskApprovedIntentEvent,
    type RiskRejectedIntentEvent,
    type StrategyIntentEvent,
} from '../core/events/EventBus';

export interface RiskManagerConfig {
    cooldownMs?: number;
    maxNotionalUsd?: number;
    volatilityRejectThreshold?: number;
}

interface SymbolRiskState {
    lastDecisionTs?: number;
    lastContext?: MarketContextEvent;
}

export interface RiskStateSnapshot {
    lastDecisionTs: Record<string, number>;
}

export class RiskManager {
    private readonly bus: EventBus;
    private readonly config: Required<RiskManagerConfig>;
    private started = false;
    private controlState: ControlState = {
        meta: { source: 'system', ts: 0 },
        mode: 'PAPER',
        paused: false,
        lifecycle: 'RUNNING',
        startedAt: 0,
        lastCommandAt: 0,
    };
    private unsubscribeIntent?: () => void;
    private unsubscribeControl?: () => void;
    private unsubscribeContext?: () => void;
    private unsubscribeReadiness?: () => void;
    private readonly state = new Map<string, SymbolRiskState>();
    private lastReadiness?: MarketDataStatusPayload;

    constructor(bus: EventBus = defaultEventBus, config: RiskManagerConfig = {}) {
        this.bus = bus;
        this.config = {
            cooldownMs: config.cooldownMs ?? 10_000,
            maxNotionalUsd: config.maxNotionalUsd ?? 50,
            volatilityRejectThreshold: config.volatilityRejectThreshold ?? 0.002,
        };
    }

    start(): void {
        if (this.started) return;
        const onIntent = (intent: StrategyIntentEvent) => this.handleIntent(intent);
        const onControl = (state: ControlState) => {
            this.controlState = state;
        };
        const onContext = (ctx: MarketContextEvent) => this.handleContext(ctx);
        const onReadiness = (status: MarketDataStatusPayload) => {
            this.lastReadiness = status;
        };
        this.bus.subscribe('strategy:intent', onIntent);
        this.bus.subscribe('control:state', onControl);
        this.bus.subscribe('analytics:context', onContext);
        this.bus.subscribe('system:market_data_status', onReadiness);
        this.unsubscribeIntent = () => this.bus.unsubscribe('strategy:intent', onIntent);
        this.unsubscribeControl = () => this.bus.unsubscribe('control:state', onControl);
        this.unsubscribeContext = () => this.bus.unsubscribe('analytics:context', onContext);
        this.unsubscribeReadiness = () => this.bus.unsubscribe('system:market_data_status', onReadiness);
        this.started = true;
        logger.info(m('lifecycle', '[RiskManager] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribeIntent?.();
        this.unsubscribeControl?.();
        this.unsubscribeContext?.();
        this.unsubscribeReadiness?.();
        this.unsubscribeIntent = undefined;
        this.unsubscribeControl = undefined;
        this.unsubscribeContext = undefined;
        this.unsubscribeReadiness = undefined;
        this.started = false;
    }

    private handleContext(ctx: MarketContextEvent): void {
        const state = this.ensureState(ctx.symbol);
        state.lastContext = ctx;
    }

    private handleIntent(intent: StrategyIntentEvent): void {
        const symbolState = this.ensureState(intent.symbol);
        const ts = intent.meta.ts;

        const reject = (reasonCode: RiskRejectedIntentEvent['reasonCode'], reason: string) => {
            const payload: RiskRejectedIntentEvent = {
                intent,
                rejectedAtTs: ts,
                reasonCode,
                reason,
                meta: this.buildMeta(intent.meta),
            };
            this.bus.publish('risk:rejected_intent', payload);
        };

        if (this.controlState.paused) return reject('PAUSED', 'bot paused');
        if (this.controlState.lifecycle !== 'RUNNING') return reject('LIFECYCLE', `lifecycle=${this.controlState.lifecycle}`);

        if (this.lastReadiness?.warmingUp || this.lastReadiness?.degraded) {
            const reasons = this.lastReadiness?.degradedReasons?.join(',') ?? 'warmingUp';
            return reject('MARKET_DATA', `market data not ready: ${reasons}`);
        }

        const lastDecision = symbolState.lastDecisionTs;
        if (lastDecision !== undefined && ts - lastDecision < this.config.cooldownMs) {
            return reject('COOLDOWN', 'cooldown active');
        }

        if (intent.targetExposureUsd > this.config.maxNotionalUsd) {
            return reject('LIMIT', `notional>${this.config.maxNotionalUsd}`);
        }

        const ctx = symbolState.lastContext;
        if (ctx?.featuresReady && ctx.contextReady && ctx.volatility !== undefined && ctx.volatility >= this.config.volatilityRejectThreshold) {
            return reject('VOLATILITY', 'volatility too high');
        }

        const approved: RiskApprovedIntentEvent = {
            intent,
            approvedAtTs: ts,
            riskVersion: 'v0',
            meta: this.buildMeta(intent.meta),
        };
        this.bus.publish('risk:approved_intent', approved);
        symbolState.lastDecisionTs = ts;
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'risk', { ts: parent.ts });
    }

    private ensureState(symbol: string): SymbolRiskState {
        const existing = this.state.get(symbol);
        if (existing) return existing;
        const fresh: SymbolRiskState = {};
        this.state.set(symbol, fresh);
        return fresh;
    }

    getState(): RiskStateSnapshot {
        const lastDecisionTs: Record<string, number> = {};
        for (const [symbol, st] of this.state.entries()) {
            if (st.lastDecisionTs !== undefined) lastDecisionTs[symbol] = st.lastDecisionTs;
        }
        return { lastDecisionTs };
    }

    setState(state: RiskStateSnapshot): void {
        this.state.clear();
        const snapshot = state?.lastDecisionTs ?? {};
        for (const [symbol, ts] of Object.entries(snapshot)) {
            this.state.set(symbol, { lastDecisionTs: ts });
        }
    }
}
