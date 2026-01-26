import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    createMeta,
    eventBus as defaultEventBus,
    type DataSourceDegraded,
    type DataSourceRecovered,
    type EventBus,
    type MarketCvdAggEvent,
    type MarketFundingAggEvent,
    type MarketLiquidationsAggEvent,
    type MarketLiquidityAggEvent,
    type MarketOpenInterestAggEvent,
    type MarketPriceCanonicalEvent,
    type MarketPriceIndexEvent,
    type MarketVolumeAggEvent,
} from '../core/events/EventBus';
import type { GlobalDataProviderClient, GlobalDataProviderEvent, ProviderError, ProviderHealth } from './types';

export interface GlobalDataGatewayOptions {
    degradedAfterErrors?: number;
}

interface ProviderState extends ProviderHealth {}

export class GlobalDataGateway {
    private readonly bus: EventBus;
    private readonly providers: GlobalDataProviderClient[];
    private readonly options: Required<GlobalDataGatewayOptions>;
    private started = false;
    private readonly providerState = new Map<string, ProviderState>();

    constructor(
        providers: GlobalDataProviderClient[] = [],
        bus: EventBus = defaultEventBus,
        options: GlobalDataGatewayOptions = {}
    ) {
        this.providers = providers;
        this.bus = bus;
        this.options = {
            degradedAfterErrors: Math.max(1, options.degradedAfterErrors ?? 3),
        };
    }

    start(): void {
        if (this.started) return;
        for (const provider of this.providers) {
            provider.start({
                onEvent: (event) => this.onProviderEvent(provider, event),
                onError: (error) => this.onProviderError(provider, error),
            });
            this.providerState.set(provider.providerId, provider.getHealth());
        }
        this.started = true;
        logger.info(m('lifecycle', '[GlobalDataGateway] started'));
    }

    stop(): void {
        if (!this.started) return;
        for (const provider of this.providers) {
            provider.stop();
        }
        this.started = false;
    }

    private onProviderEvent(provider: GlobalDataProviderClient, event: GlobalDataProviderEvent): void {
        const payload = event.payload;
        switch (event.topic) {
            case 'market:oi_agg':
                this.bus.publish('market:oi_agg', payload as MarketOpenInterestAggEvent);
                break;
            case 'market:funding_agg':
                this.bus.publish('market:funding_agg', payload as MarketFundingAggEvent);
                break;
            case 'market:liquidations_agg':
                this.bus.publish('market:liquidations_agg', payload as MarketLiquidationsAggEvent);
                break;
            case 'market:liquidity_agg':
                this.bus.publish('market:liquidity_agg', payload as MarketLiquidityAggEvent);
                break;
            case 'market:volume_agg':
                this.bus.publish('market:volume_agg', payload as MarketVolumeAggEvent);
                break;
            case 'market:cvd_spot_agg':
                this.bus.publish('market:cvd_spot_agg', payload as MarketCvdAggEvent);
                break;
            case 'market:cvd_futures_agg':
                this.bus.publish('market:cvd_futures_agg', payload as MarketCvdAggEvent);
                break;
            case 'market:price_index':
                this.bus.publish('market:price_index', payload as MarketPriceIndexEvent);
                break;
            case 'market:price_canonical':
                this.bus.publish('market:price_canonical', payload as MarketPriceCanonicalEvent);
                break;
            default:
                logger.warn(m('warn', `[GlobalDataGateway] unsupported topic=${event.topic}`));
                return;
        }
        this.recordSuccess(provider.providerId, payload.ts);
    }

    private onProviderError(provider: GlobalDataProviderClient, error: ProviderError): void {
        this.recordError(provider.providerId, error.ts, error.reason);
    }

    private recordSuccess(providerId: string, ts: number): void {
        const state = this.ensureState(providerId);
        const wasDegraded = state.degraded;
        state.lastSuccessTs = ts;
        state.consecutiveErrors = 0;
        if (wasDegraded) {
            state.degraded = false;
            const evt: DataSourceRecovered = {
                sourceId: providerId,
                recoveredTs: ts,
                lastErrorTs: state.lastErrorTs,
                meta: createMeta('global_data', { ts }),
            };
            this.bus.publish('data:sourceRecovered', evt);
        }
    }

    private recordError(providerId: string, ts: number, reason: string): void {
        const state = this.ensureState(providerId);
        state.lastErrorTs = ts;
        state.consecutiveErrors += 1;
        if (state.degraded) return;
        if (state.consecutiveErrors < this.options.degradedAfterErrors) return;
        state.degraded = true;
        const evt: DataSourceDegraded = {
            sourceId: providerId,
            reason,
            lastSuccessTs: state.lastSuccessTs,
            consecutiveErrors: state.consecutiveErrors,
            meta: createMeta('global_data', { ts }),
        };
        this.bus.publish('data:sourceDegraded', evt);
        logger.warn(m('warn', `[GlobalDataGateway] source degraded provider=${providerId} reason=${reason}`));
    }

    private ensureState(providerId: string): ProviderState {
        const existing = this.providerState.get(providerId);
        if (existing) return existing;
        const fresh: ProviderState = {
            providerId,
            consecutiveErrors: 0,
            degraded: false,
        };
        this.providerState.set(providerId, fresh);
        return fresh;
    }
}
