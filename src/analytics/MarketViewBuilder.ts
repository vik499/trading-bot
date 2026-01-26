import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsFeaturesEvent,
    type AnalyticsFlowEvent,
    type AnalyticsLiquidityEvent,
    type AnalyticsMarketViewEvent,
    type EventBus,
    type EventMeta,
    type MarketCvdAggEvent,
    type MarketFundingAggEvent,
    type MarketLiquidationsAggEvent,
    type MarketLiquidityAggEvent,
    type MarketOpenInterestAggEvent,
    type MarketPriceIndexEvent,
    type MarketVolumeAggEvent,
    type MarketViewFlow,
    type MarketViewLiquidity,
    type MarketViewMacro,
    type MarketViewMicro,
} from '../core/events/EventBus';

export interface MarketViewConfig {
    microTfs?: string[];
    macroTfs?: string[];
}

interface MarketViewState {
    micro?: MarketViewMicro;
    macro?: MarketViewMacro;
    flow?: MarketViewFlow;
    liquidity?: MarketViewLiquidity;
    sourceTopics: Set<string>;
}

export class MarketViewBuilder {
    private readonly bus: EventBus;
    private readonly config: Required<MarketViewConfig>;
    private started = false;
    private readonly unsubscribers: Array<() => void> = [];
    private readonly stateBySymbol = new Map<string, MarketViewState>();

    constructor(bus: EventBus = defaultEventBus, config: MarketViewConfig = {}) {
        this.bus = bus;
        this.config = {
            microTfs: config.microTfs ?? ['5m'],
            macroTfs: config.macroTfs ?? ['1h', '4h', '1d'],
        };
    }

    start(): void {
        if (this.started) return;

        const onFeatures = (evt: AnalyticsFeaturesEvent) => this.onFeatures(evt);
        const onFlow = (evt: AnalyticsFlowEvent) => this.onFlow(evt);
        const onLiquidity = (evt: AnalyticsLiquidityEvent) => this.onLiquidity(evt);
        const onOiAgg = (evt: MarketOpenInterestAggEvent) => this.onOiAgg(evt);
        const onFundingAgg = (evt: MarketFundingAggEvent) => this.onFundingAgg(evt);
        const onLiquidationsAgg = (evt: MarketLiquidationsAggEvent) => this.onLiquidationsAgg(evt);
        const onVolumeAgg = (evt: MarketVolumeAggEvent) => this.onVolumeAgg(evt);
        const onCvdSpotAgg = (evt: MarketCvdAggEvent) => this.onCvdSpotAgg(evt);
        const onCvdFuturesAgg = (evt: MarketCvdAggEvent) => this.onCvdFuturesAgg(evt);
        const onPriceIndex = (evt: MarketPriceIndexEvent) => this.onPriceIndex(evt);
        const onLiquidityAgg = (evt: MarketLiquidityAggEvent) => this.onLiquidityAgg(evt);

        this.bus.subscribe('analytics:features', onFeatures);
        this.bus.subscribe('analytics:flow', onFlow);
        this.bus.subscribe('analytics:liquidity', onLiquidity);
        this.bus.subscribe('market:oi_agg', onOiAgg);
        this.bus.subscribe('market:funding_agg', onFundingAgg);
        this.bus.subscribe('market:liquidations_agg', onLiquidationsAgg);
        this.bus.subscribe('market:volume_agg', onVolumeAgg);
        this.bus.subscribe('market:cvd_spot_agg', onCvdSpotAgg);
        this.bus.subscribe('market:cvd_futures_agg', onCvdFuturesAgg);
        this.bus.subscribe('market:price_index', onPriceIndex);
        this.bus.subscribe('market:liquidity_agg', onLiquidityAgg);

        this.unsubscribers.push(
            () => this.bus.unsubscribe('analytics:features', onFeatures),
            () => this.bus.unsubscribe('analytics:flow', onFlow),
            () => this.bus.unsubscribe('analytics:liquidity', onLiquidity),
            () => this.bus.unsubscribe('market:oi_agg', onOiAgg),
            () => this.bus.unsubscribe('market:funding_agg', onFundingAgg),
            () => this.bus.unsubscribe('market:liquidations_agg', onLiquidationsAgg),
            () => this.bus.unsubscribe('market:volume_agg', onVolumeAgg),
            () => this.bus.unsubscribe('market:cvd_spot_agg', onCvdSpotAgg),
            () => this.bus.unsubscribe('market:cvd_futures_agg', onCvdFuturesAgg),
            () => this.bus.unsubscribe('market:price_index', onPriceIndex),
            () => this.bus.unsubscribe('market:liquidity_agg', onLiquidityAgg)
        );

        this.started = true;
        logger.info(m('lifecycle', '[MarketViewBuilder] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers.length = 0;
        this.started = false;
    }

    private onFeatures(evt: AnalyticsFeaturesEvent): void {
        const symbol = evt.symbol;
        const state = this.ensureState(symbol);
        const tf = evt.klineTf ?? 'tick';

        if (this.config.microTfs.includes(tf) || evt.sourceTopic === 'market:ticker') {
            state.micro = {
                tf,
                close: evt.lastPrice,
                emaFast: evt.emaFast,
                emaSlow: evt.emaSlow,
                rsi: evt.rsi,
                atr: evt.atr,
            };
            state.sourceTopics.add('analytics:features');
            this.publish(symbol, evt.meta, state);
            return;
        }

        if (this.config.macroTfs.includes(tf)) {
            const close = evt.lastPrice;
            const atr = evt.atr;
            const atrPct = close && atr !== undefined && close !== 0 ? atr / close : undefined;
            state.macro = {
                tf,
                close,
                emaFast: evt.emaFast,
                emaSlow: evt.emaSlow,
                atr,
                atrPct,
            };
            state.sourceTopics.add('analytics:features');
            this.publish(symbol, evt.meta, state);
        }
    }

    private onFlow(evt: AnalyticsFlowEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            cvdSpotAgg: evt.cvdSpot,
            cvdFuturesAgg: evt.cvdFutures,
            oiAgg: evt.oi ?? evt.oiDelta,
            fundingAgg: evt.fundingRate,
        };
        state.sourceTopics.add('analytics:flow');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onLiquidity(evt: AnalyticsLiquidityEvent): void {
        const state = this.ensureState(evt.symbol);
        state.liquidity = {
            bestBid: evt.bestBid,
            bestAsk: evt.bestAsk,
            spread: evt.spread,
            depthBid: evt.depthBid,
            depthAsk: evt.depthAsk,
            imbalance: evt.imbalance,
        };
        state.sourceTopics.add('analytics:liquidity');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onLiquidityAgg(evt: MarketLiquidityAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.liquidity = {
            bestBid: evt.bestBid,
            bestAsk: evt.bestAsk,
            spread: evt.spread,
            depthBid: evt.depthBid,
            depthAsk: evt.depthAsk,
            imbalance: evt.imbalance,
        };
        state.sourceTopics.add('market:liquidity_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onOiAgg(evt: MarketOpenInterestAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            oiAgg: evt.openInterest,
        };
        state.sourceTopics.add('market:oi_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onFundingAgg(evt: MarketFundingAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            fundingAgg: evt.fundingRate,
        };
        state.sourceTopics.add('market:funding_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onLiquidationsAgg(evt: MarketLiquidationsAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            liquidationsAgg: evt.liquidationNotional,
        };
        state.sourceTopics.add('market:liquidations_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onVolumeAgg(evt: MarketVolumeAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            spotVolumeAgg: evt.spotVolumeUsd,
            futuresVolumeAgg: evt.futuresVolumeUsd,
        };
        state.sourceTopics.add('market:volume_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onCvdSpotAgg(evt: MarketCvdAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            cvdSpotAgg: evt.cvd,
        };
        state.sourceTopics.add('market:cvd_spot_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onCvdFuturesAgg(evt: MarketCvdAggEvent): void {
        const state = this.ensureState(evt.symbol);
        state.flow = {
            ...state.flow,
            cvdFuturesAgg: evt.cvd,
        };
        state.sourceTopics.add('market:cvd_futures_agg');
        this.publish(evt.symbol, evt.meta, state);
    }

    private onPriceIndex(evt: MarketPriceIndexEvent): void {
        const state = this.ensureState(evt.symbol);
        if (state.micro) state.micro.indexPrice = evt.indexPrice;
        if (state.macro) state.macro.indexPrice = evt.indexPrice;
        state.sourceTopics.add('market:price_index');
        this.publish(evt.symbol, evt.meta, state);
    }

    private publish(symbol: string, parentMeta: EventMeta, state: MarketViewState): void {
        const payload: AnalyticsMarketViewEvent = {
            symbol,
            ts: parentMeta.ts,
            micro: state.micro,
            macro: state.macro,
            flow: state.flow,
            liquidity: state.liquidity,
            sourceTopics: Array.from(state.sourceTopics),
            meta: inheritMeta(parentMeta, 'analytics', { ts: parentMeta.ts }),
        };
        this.bus.publish('analytics:market_view', payload);
    }

    private ensureState(symbol: string): MarketViewState {
        const existing = this.stateBySymbol.get(symbol);
        if (existing) return existing;
        const fresh: MarketViewState = {
            sourceTopics: new Set(),
        };
        this.stateBySymbol.set(symbol, fresh);
        return fresh;
    }
}
