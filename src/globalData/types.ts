import type {
    MarketCvdAggEvent,
    MarketFundingAggEvent,
    MarketLiquidationsAggEvent,
    MarketLiquidityAggEvent,
    MarketOpenInterestAggEvent,
    MarketPriceCanonicalEvent,
    MarketPriceIndexEvent,
    MarketVolumeAggEvent,
} from '../core/events/EventBus';

export type GlobalAggTopic =
    | 'market:oi_agg'
    | 'market:funding_agg'
    | 'market:liquidations_agg'
    | 'market:liquidity_agg'
    | 'market:volume_agg'
    | 'market:cvd_agg'
    | 'market:cvd_spot_agg'
    | 'market:cvd_futures_agg'
    | 'market:price_index'
    | 'market:price_canonical';

export interface GlobalAggPayloadMap {
    'market:oi_agg': MarketOpenInterestAggEvent;
    'market:funding_agg': MarketFundingAggEvent;
    'market:liquidations_agg': MarketLiquidationsAggEvent;
    'market:liquidity_agg': MarketLiquidityAggEvent;
    'market:volume_agg': MarketVolumeAggEvent;
    'market:cvd_agg': MarketCvdAggEvent;
    'market:cvd_spot_agg': MarketCvdAggEvent;
    'market:cvd_futures_agg': MarketCvdAggEvent;
    'market:price_index': MarketPriceIndexEvent;
    'market:price_canonical': MarketPriceCanonicalEvent;
}

export interface GlobalDataProviderEvent<T extends GlobalAggTopic = GlobalAggTopic> {
    topic: T;
    payload: GlobalAggPayloadMap[T];
}

export interface ProviderError {
    providerId: string;
    ts: number;
    reason: string;
    error?: unknown;
}

export interface ProviderHealth {
    providerId: string;
    lastSuccessTs?: number;
    lastErrorTs?: number;
    consecutiveErrors: number;
    degraded: boolean;
    backoffMs?: number;
}

export interface GlobalDataProviderClient {
    providerId: string;
    start(handlers: {
        onEvent: (event: GlobalDataProviderEvent) => void;
        onError: (error: ProviderError) => void;
    }): void;
    stop(): void;
    getHealth(): ProviderHealth;
}
