import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsFeaturesEvent,
    type AnalyticsMarketViewEvent,
    type EventBus,
    type ResearchPatternFound,
    type ResearchPatternStats,
} from '../core/events/EventBus';

export interface PatternMinerConfig {
    windowSize?: number;
    emitInterval?: number;
    sourceTopic?: 'analytics:features' | 'analytics:market_view';
}

interface PatternState {
    values: number[];
    lastEmitCount: number;
    patternCounts: Record<string, number>;
    patternAvgReturn: Record<string, number>;
}

export class PatternMiner {
    private readonly bus: EventBus;
    private readonly config: Required<PatternMinerConfig>;
    private started = false;
    private unsubscribe?: () => void;
    private readonly stateBySymbol = new Map<string, PatternState>();

    constructor(bus: EventBus = defaultEventBus, config: PatternMinerConfig = {}) {
        this.bus = bus;
        this.config = {
            windowSize: Math.max(5, config.windowSize ?? 20),
            emitInterval: Math.max(1, config.emitInterval ?? 10),
            sourceTopic: config.sourceTopic ?? 'analytics:features',
        };
    }

    start(): void {
        if (this.started) return;
        if (this.config.sourceTopic === 'analytics:market_view') {
            const handler = (evt: AnalyticsMarketViewEvent) => this.onMarketView(evt);
            this.bus.subscribe('analytics:market_view', handler);
            this.unsubscribe = () => this.bus.unsubscribe('analytics:market_view', handler);
        } else {
            const handler = (evt: AnalyticsFeaturesEvent) => this.onFeatures(evt);
            this.bus.subscribe('analytics:features', handler);
            this.unsubscribe = () => this.bus.unsubscribe('analytics:features', handler);
        }
        this.started = true;
        logger.info(m('lifecycle', '[PatternMiner] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    private onFeatures(evt: AnalyticsFeaturesEvent): void {
        const price = evt.lastPrice;
        this.process(evt.symbol, price, evt.meta.ts, evt.meta, 'analytics:features');
    }

    private onMarketView(evt: AnalyticsMarketViewEvent): void {
        const price = evt.micro?.close ?? evt.macro?.close;
        if (price === undefined) return;
        this.process(evt.symbol, price, evt.meta.ts, evt.meta, 'analytics:market_view');
    }

    private process(
        symbol: string,
        price: number,
        ts: number,
        meta: AnalyticsFeaturesEvent['meta'],
        sourceTopic: 'analytics:features' | 'analytics:market_view'
    ): void {
        const state = this.ensureState(symbol);
        state.values.push(price);
        if (state.values.length < this.config.windowSize) return;
        if (state.values.length > this.config.windowSize) {
            state.values.shift();
        }

        state.lastEmitCount += 1;
        if (state.lastEmitCount % this.config.emitInterval !== 0) return;

        const slope = this.computeSlope(state.values);
        const patternId = slope >= 0 ? 'trend_up' : 'trend_down';
        const score = Math.abs(slope);
        const windowReturn = state.values.length >= 2 ? (state.values[state.values.length - 1] - state.values[0]) / state.values[0] : 0;

        state.patternCounts[patternId] = (state.patternCounts[patternId] ?? 0) + 1;
        const prevAvg = state.patternAvgReturn[patternId] ?? 0;
        const count = state.patternCounts[patternId];
        state.patternAvgReturn[patternId] = prevAvg + (windowReturn - prevAvg) / count;

        const found: ResearchPatternFound = {
            symbol,
            ts,
            patternId,
            score,
            window: this.config.windowSize,
            sourceTopic,
            meta: inheritMeta(meta, 'research', { ts }),
        };
        this.bus.publish('research:patternFound', found);

        const stats: ResearchPatternStats = {
            symbol,
            ts,
            patternId,
            occurrences: count,
            avgReturn: state.patternAvgReturn[patternId],
            meta: inheritMeta(meta, 'research', { ts }),
        };
        this.bus.publish('research:patternStats', stats);
    }

    private computeSlope(values: number[]): number {
        if (values.length < 2) return 0;
        return values[values.length - 1] - values[0];
    }

    private ensureState(symbol: string): PatternState {
        const existing = this.stateBySymbol.get(symbol);
        if (existing) return existing;
        const fresh: PatternState = {
            values: [],
            lastEmitCount: 0,
            patternCounts: {},
            patternAvgReturn: {},
        };
        this.stateBySymbol.set(symbol, fresh);
        return fresh;
    }
}
