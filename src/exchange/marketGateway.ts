import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    asTsMs,
    createMeta,
    eventBus,
    inheritMeta,
    nowMs,
    type EventMeta,
    type EventBus,
    type Kline,
    type KlineBootstrapCompleted,
    type KlineBootstrapFailed,
    type KlineBootstrapRequested,
    type KlineEvent,
    type KlineInterval,
    type MarketConnectRequest,
    type MarketConnected,
    type MarketDisconnectRequest,
    type MarketDisconnected,
    type MarketErrorEvent,
    type MarketResyncRequested,
    type MarketSubscribeRequest,
    type KnownMarketType,
    type MarketType,
    type VenueId,
} from '../core/events/EventBus';
import type { OpenInterestCollector } from '../core/market/OpenInterestCollector';
import { inferMarketTypeFromStreamId } from '../core/market/symbols';
import { BybitDerivativesRestPoller, BybitPublicRestClient, type KlineFetchResult } from './bybit/restClient';

export interface MarketWsClient {
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    subscribeTicker(symbol: string): void;
    subscribeTrades(symbol: string): void;
    subscribeOrderbook(symbol: string, depth?: number): void;
    subscribeKlines?(symbol: string, interval: KlineInterval): void;
    subscribeLiquidations?(symbol: string): void;
    isAlive(): boolean;
}

export interface KlineRestClient {
    fetchKlines(params: { symbol: string; interval: KlineInterval; limit?: number; sinceTs?: number }): Promise<KlineFetchResult>;
    streamId?: string;
    marketType?: MarketType;
}

export interface MarketGatewayOptions {
    enableKlineBootstrap?: boolean;
    enableResync?: boolean;
    resyncStrategy?: 'reconnect' | 'ignore';
    venue?: VenueId;
    marketType?: KnownMarketType;
    resyncCooldownMs?: number;
    resyncLogCooldownMs?: number;
    resyncReasonCooldownMs?: number;
    topicFilter?: (topic: string) => boolean;
    shutdownTimeoutMs?: number;
}

/**
 * MarketGateway связывает события EventBus с конкретной WS-реализацией.
 * Цель: держать WS lifecycle событийным и переиспользуемым.
 */
export class MarketGateway {
    private started = false;
    private readonly unsubscribers: Array<() => void> = [];
    private readonly bus: EventBus;
    private readonly derivativesPoller: OpenInterestCollector;
    private readonly emptyBootstrapLogAt = new Map<string, number>();
    private readonly enableKlineBootstrap: boolean;
    private readonly enableResync: boolean;
    private readonly resyncStrategy: 'reconnect' | 'ignore';
    private readonly venue?: VenueId;
    private readonly marketType?: KnownMarketType;
    private readonly resyncCooldownMs: number;
    private readonly resyncLogCooldownMs: number;
    private readonly resyncReasonCooldownMs: number;
    private readonly okxEnableKlines: boolean;
    private readonly topicFilter?: (topic: string) => boolean;
    private readonly shutdownTimeoutMs: number;
    private lastSubscriptions: string[] = [];
    private resyncInFlight = false;
    private readonly lastResyncAt = new Map<string, number>();
    private readonly lastResyncLogAt = new Map<string, number>();

    constructor(
        private readonly wsClient: MarketWsClient,
        private readonly restClient: KlineRestClient = new BybitPublicRestClient(),
        bus: EventBus = eventBus,
        derivativesPoller?: OpenInterestCollector,
        private readonly options: MarketGatewayOptions = {}
    ) {
        this.bus = bus;
        this.derivativesPoller = derivativesPoller ?? new BybitDerivativesRestPoller(new BybitPublicRestClient(), bus);
        this.enableKlineBootstrap = this.options.enableKlineBootstrap ?? true;
        this.enableResync = this.options.enableResync ?? true;
        this.resyncStrategy = this.options.resyncStrategy ?? 'reconnect';
        this.venue = this.options.venue;
        this.marketType = this.options.marketType;
        this.resyncCooldownMs = Math.max(0, this.options.resyncCooldownMs ?? 5_000);
        this.resyncLogCooldownMs = Math.max(0, this.options.resyncLogCooldownMs ?? 5_000);
        this.resyncReasonCooldownMs = Math.max(0, this.options.resyncReasonCooldownMs ?? 2_000);
        this.okxEnableKlines = readFlagFromEnv('OKX_ENABLE_KLINES', true);
        this.topicFilter = this.options.topicFilter;
        this.shutdownTimeoutMs = Math.max(0, this.options.shutdownTimeoutMs ?? 2_000);
    }

    start(): void {
        if (this.started) return;

        const connectHandler = (payload: MarketConnectRequest) => {
            void this.handleConnect(payload);
        };
        const disconnectHandler = (payload: MarketDisconnectRequest) => {
            void this.handleDisconnect(payload);
        };
        const subscribeHandler = (payload: MarketSubscribeRequest) => {
            void this.handleSubscribe(payload);
        };
        const bootstrapHandler = (payload: KlineBootstrapRequested) => {
            void this.handleKlineBootstrap(payload);
        };
        const resyncHandler = (payload: MarketResyncRequested) => {
            void this.handleResync(payload);
        };

        this.bus.subscribe('market:connect', connectHandler);
        this.bus.subscribe('market:disconnect', disconnectHandler);
        this.bus.subscribe('market:subscribe', subscribeHandler);
        if (this.enableKlineBootstrap) {
            this.bus.subscribe('market:kline_bootstrap_requested', bootstrapHandler);
        }
        this.bus.subscribe('market:resync_requested', resyncHandler);

        this.unsubscribers.push(
            () => this.bus.unsubscribe('market:connect', connectHandler),
            () => this.bus.unsubscribe('market:disconnect', disconnectHandler),
            () => this.bus.unsubscribe('market:subscribe', subscribeHandler),
            () => this.bus.unsubscribe('market:kline_bootstrap_requested', bootstrapHandler),
            () => this.bus.unsubscribe('market:resync_requested', resyncHandler)
        );

        this.started = true;
    }

    stop(): void {
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers.length = 0;
        this.derivativesPoller.stop();
        this.started = false;
    }

    async shutdown(reason = 'shutdown'): Promise<void> {
        this.derivativesPoller.stop();
        const disconnect = this.wsClient.disconnect();
        if (this.shutdownTimeoutMs > 0) {
            await Promise.race([
                disconnect,
                new Promise<void>((resolve) => setTimeout(resolve, this.shutdownTimeoutMs)),
            ]);
        } else {
            await disconnect;
        }
        this.publishDisconnected(createMeta('market'), reason);
    }

    private async handleConnect(payload: MarketConnectRequest): Promise<void> {
        if (payload.venue && this.venue && payload.venue !== this.venue) return;
        if (payload.marketType && this.marketType && payload.marketType !== this.marketType) return;
        const url = payload.url;
        try {
            await this.wsClient.connect();
            const connected: MarketConnected = {
                meta: inheritMeta(payload.meta, 'market'),
                url,
            };
            this.bus.publish('market:connected', connected);
            logger.info(m('ok', `[MarketGateway] connected${url ? ` to ${url}` : ''}`));

            if (payload.subscriptions?.length) {
                this.lastSubscriptions = [...payload.subscriptions];
                await this.subscribeTopics(payload.meta, payload.subscriptions);
            }
        } catch (err) {
            this.publishError(payload.meta, 'connect', err);
        }
    }

    private async handleDisconnect(payload: MarketDisconnectRequest): Promise<void> {
        try {
            await this.wsClient.disconnect();
            this.derivativesPoller.stop();
            this.publishDisconnected(payload.meta, payload.reason);
        } catch (err) {
            this.publishError(payload.meta, 'disconnect', err);
        }
    }

    private async handleSubscribe(payload: MarketSubscribeRequest): Promise<void> {
        if (payload.venue && this.venue && payload.venue !== this.venue) return;
        if (payload.marketType && this.marketType && payload.marketType !== this.marketType) return;
        try {
            this.lastSubscriptions = [...payload.topics];
            await this.subscribeTopics(payload.meta, payload.topics);
        } catch (err) {
            this.publishError(payload.meta, 'subscribe', err);
        }
    }

    private async handleResync(payload: MarketResyncRequested): Promise<void> {
        if (!this.enableResync) return;
        if (this.venue && payload.venue !== this.venue) return;
        if (this.resyncStrategy === 'ignore') {
            const nowTs = payload.meta.ts;
            const key = `${payload.venue}:${payload.symbol}:${payload.channel}`;
            const lastLogTs = this.lastResyncLogAt.get(key) ?? 0;
            if (nowTs - lastLogTs >= this.resyncLogCooldownMs) {
                this.lastResyncLogAt.set(key, nowTs);
                logger.infoThrottled(
                    `resync:ignored:${key}`,
                    m('warn', `[MarketGateway] resync ignored venue=${payload.venue} symbol=${payload.symbol} reason=${payload.reason}`),
                    this.resyncLogCooldownMs
                );
            }
            return;
        }
        const meta = payload.meta;
        const key = `${payload.venue}:${payload.symbol}:${payload.channel}`;
        const reasonKey = `${payload.venue}:${payload.symbol}:${payload.channel}:${payload.reason}`;
        const nowTs = meta.ts;
        const lastReasonTs = this.lastResyncAt.get(reasonKey) ?? 0;
        if (this.resyncReasonCooldownMs > 0 && lastReasonTs > 0 && nowTs - lastReasonTs < this.resyncReasonCooldownMs) {
            const lastLogTs = this.lastResyncLogAt.get(reasonKey) ?? 0;
            if (nowTs - lastLogTs >= this.resyncLogCooldownMs) {
                this.lastResyncLogAt.set(reasonKey, nowTs);
                logger.warn(
                    m('warn', `[MarketGateway] resync throttled venue=${payload.venue} symbol=${payload.symbol} reason=${payload.reason}`)
                );
            }
            return;
        }
        const lastResyncTs = this.lastResyncAt.get(key) ?? 0;
        if (this.resyncInFlight || (nowTs - lastResyncTs < this.resyncCooldownMs)) {
            const lastLogTs = this.lastResyncLogAt.get(key) ?? 0;
            if (nowTs - lastLogTs >= this.resyncLogCooldownMs) {
                this.lastResyncLogAt.set(key, nowTs);
                logger.warn(
                    m('warn', `[MarketGateway] resync throttled venue=${payload.venue} symbol=${payload.symbol} reason=${payload.reason}`)
                );
            }
            return;
        }

        this.resyncInFlight = true;
        this.lastResyncAt.set(key, nowTs);
        this.lastResyncAt.set(reasonKey, nowTs);
        logger.warn(m('warn', `[MarketGateway] resync requested venue=${payload.venue} symbol=${payload.symbol} reason=${payload.reason}`));
        try {
            await this.wsClient.disconnect();
            this.publishDisconnected(meta, `resync:${payload.reason}`);
            await this.wsClient.connect();
            this.bus.publish('market:connected', { meta: inheritMeta(meta, 'market'), details: { reason: 'resync' } });
            if (this.lastSubscriptions.length) {
                await this.subscribeTopics(meta, this.lastSubscriptions);
            }
        } catch (err) {
            this.publishError(meta, 'disconnect', err);
        } finally {
            this.resyncInFlight = false;
        }
    }

    private async handleKlineBootstrap(payload: KlineBootstrapRequested): Promise<void> {
        if (payload.venue && this.venue && payload.venue !== this.venue) return;
        if (payload.marketType && this.marketType && payload.marketType !== this.marketType) return;
        const startedAt = payload.meta.ts;
        try {
            const result = await this.restClient.fetchKlines({
                symbol: payload.symbol,
                interval: payload.interval,
                limit: payload.limit,
                sinceTs: payload.sinceTs,
            });
            const klines = result.klines;
            const ordered = [...klines].sort((a, b) => a.startTs - b.startTs);

            if (ordered.length === 0) {
                this.logEmptyBootstrap(payload, result.meta);
                const failed: KlineBootstrapFailed = {
                    symbol: payload.symbol,
                    interval: payload.interval,
                    tf: payload.tf,
                    reason: 'kline bootstrap returned empty list',
                    error: result.meta,
                    meta: inheritMeta(payload.meta, 'market'),
                };
                this.bus.publish('market:kline_bootstrap_failed', failed);
                return;
            }

            const sourceId = normalizeKlineSourceId(this.restClient.streamId ?? 'unknown');
            const marketType =
                payload.marketType ??
                this.asKnownMarketType(this.restClient.marketType) ??
                this.asKnownMarketType(inferMarketTypeFromStreamId(sourceId)) ??
                'futures';
            for (const kline of ordered) {
                const event: KlineEvent = {
                    ...kline,
                    tf: kline.tf ?? payload.tf,
                    streamId: sourceId,
                    marketType,
                    meta: createMeta('market', {
                        tsEvent: asTsMs(kline.endTs),
                        tsIngest: asTsMs(nowMs()),
                        correlationId: payload.meta.correlationId,
                        streamId: sourceId,
                    }),
                };
                this.bus.publish('market:kline', event);
            }

            const rangeStart = ordered.length ? ordered[0].startTs : 0;
            const rangeEnd = ordered.length ? ordered[ordered.length - 1].endTs : 0;
            const durationMs = Math.max(0, rangeEnd - rangeStart);
            const completed: KlineBootstrapCompleted = {
                symbol: payload.symbol,
                interval: payload.interval,
                tf: payload.tf,
                received: ordered.length,
                emitted: ordered.length,
                startTs: rangeStart,
                endTs: rangeEnd,
                durationMs,
                meta: inheritMeta(payload.meta, 'market'),
            };
            this.bus.publish('market:kline_bootstrap_completed', completed);
        } catch (err) {
            const failed: KlineBootstrapFailed = {
                symbol: payload.symbol,
                interval: payload.interval,
                tf: payload.tf,
                reason: err instanceof Error ? err.message : String(err),
                error: err,
                meta: inheritMeta(payload.meta, 'market'),
            };
            this.bus.publish('market:kline_bootstrap_failed', failed);
        }
    }

    private logEmptyBootstrap(payload: KlineBootstrapRequested, meta: KlineFetchResult['meta']): void {
        const key = `${payload.symbol}:${payload.tf}`;
        const nowTs = payload.meta.ts;
        const last = this.emptyBootstrapLogAt.get(key) ?? 0;
        if (nowTs - last < 30_000) return;
        this.emptyBootstrapLogAt.set(key, nowTs);

        const intervalMs = intervalToMs(payload.interval);
        const startTs = payload.sinceTs;
        const endTs =
            startTs !== undefined && intervalMs !== undefined && payload.limit !== undefined
                ? startTs + intervalMs * payload.limit
                : undefined;

        const parts = [
            `symbol=${payload.symbol}`,
            `tf=${payload.tf}`,
            `interval=${payload.interval}`,
            `limit=${payload.limit ?? 'n/a'}`,
            startTs !== undefined ? `startTs=${startTs}` : undefined,
            endTs !== undefined ? `endTs=${endTs}` : undefined,
            payload.sinceTs !== undefined ? `sinceTs=${payload.sinceTs}` : undefined,
            `status=${meta.status}`,
            meta.retCode !== undefined ? `retCode=${meta.retCode}` : undefined,
            meta.retMsg ? `retMsg=${meta.retMsg}` : undefined,
            `endpoint=${meta.endpoint}`,
            `params=${JSON.stringify(meta.params)}`,
            meta.requestId ? `requestId=${meta.requestId}` : undefined,
            meta.rateLimit ? `rateLimit=${JSON.stringify(meta.rateLimit)}` : undefined,
            meta.listLength !== undefined ? `listLength=${meta.listLength}` : undefined,
        ]
            .filter(Boolean)
            .join(' ');

        logger.warn(m('warn', `[KlineBootstrap] empty response ${parts}`));
    }

    private asKnownMarketType(value?: MarketType): KnownMarketType | undefined {
        if (value === 'spot' || value === 'futures') return value;
        return undefined;
    }

    private async subscribeTopics(parentMeta: EventMeta, topics: string[]): Promise<void> {
        for (const topic of topics) {
            if (this.topicFilter && !this.topicFilter(topic)) {
                continue;
            }
            if (topic.startsWith('tickers.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.wsClient.subscribeTicker(symbol);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                    this.derivativesPoller.startForSymbol(symbol);
                }
                continue;
            }

            if (topic.startsWith('trades.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.wsClient.subscribeTrades(symbol);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                    this.derivativesPoller.startForSymbol(symbol);
                }
                continue;
            }

            if (topic.startsWith('orderbook.')) {
                const parts = topic.split('.');
                const symbol = parts[2] ?? parts[1];
                const depth = parts.length > 2 ? Number(parts[1]) : undefined;
                if (symbol) {
                    const depthValue = depth && Number.isFinite(depth) ? depth : undefined;
                    this.wsClient.subscribeOrderbook(symbol, depthValue);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                    this.derivativesPoller.startForSymbol(symbol);
                }
                continue;
            }

            if (topic.startsWith('kline.')) {
                const parts = topic.split('.');
                const interval = parts[1] as KlineInterval | undefined;
                const symbol = parts[2];
                if (interval && symbol) {
                    if (this.venue === 'okx' && !this.okxEnableKlines) {
                        logger.debug(m('connect', `[MarketGateway] skip kline for OKX (disabled) ${topic}`));
                        continue;
                    }
                    this.wsClient.subscribeKlines?.(symbol, interval);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                }
                continue;
            }

            if (topic.startsWith('liquidations.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.wsClient.subscribeLiquidations?.(symbol);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                }
                continue;
            }

            if (topic.startsWith('oi.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.derivativesPoller.startForSymbol(symbol);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                }
                continue;
            }

            if (topic.startsWith('funding.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.derivativesPoller.startForSymbol(symbol);
                    logger.debug(m('connect', `[MarketGateway] subscribe ${topic}`));
                }
                continue;
            }

            logger.warn(m('warn', `[MarketGateway] unsupported market topic: ${topic}`));
        }
    }

    private publishDisconnected(meta: EventMeta, reason?: string): void {
        const payload: MarketDisconnected = {
            meta: inheritMeta(meta, 'market'),
            reason,
        };
        this.bus.publish('market:disconnected', payload);
    }

    private publishError(meta: EventMeta, phase: MarketErrorEvent['phase'], err: unknown): void {
        const payload: MarketErrorEvent = {
            meta: inheritMeta(meta, 'market'),
            phase,
            message: err instanceof Error ? err.message : String(err),
            error: err,
        };
        this.bus.publish('market:error', payload);
        logger.error(m('error', `[MarketGateway] ${phase} failed: ${payload.message}`));
    }
}

function intervalToMs(interval: KlineInterval): number | undefined {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return undefined;
    return minutes * 60_000;
}

function normalizeKlineSourceId(streamId: string): string {
    if (streamId.endsWith('.kline')) {
        return streamId.replace('.kline', '');
    }
    return streamId;
}

function readFlagFromEnv(name: string, fallback = true): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = String(raw).trim().toLowerCase();
    if (['0', 'false', 'off', 'no'].includes(normalized)) return false;
    if (['1', 'true', 'on', 'yes'].includes(normalized)) return true;
    return fallback;
}
