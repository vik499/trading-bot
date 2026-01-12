import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    createMeta,
    eventBus,
    inheritMeta,
    type EventMeta,
    type MarketConnectRequest,
    type MarketConnected,
    type MarketDisconnectRequest,
    type MarketDisconnected,
    type MarketErrorEvent,
    type MarketSubscribeRequest,
} from '../core/events/EventBus';
import { BybitPublicWsClient } from './bybit/wsClient';

/**
 * MarketGateway связывает события EventBus с конкретной WS-реализацией.
 * Цель: держать WS lifecycle событийным и переиспользуемым.
 */
export class MarketGateway {
    private started = false;
    private readonly unsubscribers: Array<() => void> = [];

    constructor(private readonly wsClient: BybitPublicWsClient) {}

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

        eventBus.subscribe('market:connect', connectHandler);
        eventBus.subscribe('market:disconnect', disconnectHandler);
        eventBus.subscribe('market:subscribe', subscribeHandler);

        this.unsubscribers.push(
            () => eventBus.unsubscribe('market:connect', connectHandler),
            () => eventBus.unsubscribe('market:disconnect', disconnectHandler),
            () => eventBus.unsubscribe('market:subscribe', subscribeHandler)
        );

        this.started = true;
    }

    stop(): void {
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers.length = 0;
        this.started = false;
    }

    async shutdown(reason = 'shutdown'): Promise<void> {
        await this.wsClient.disconnect();
        this.publishDisconnected(createMeta('market'), reason);
    }

    private async handleConnect(payload: MarketConnectRequest): Promise<void> {
        const url = payload.url;
        try {
            await this.wsClient.connect();
            const connected: MarketConnected = {
                meta: inheritMeta(payload.meta, 'market'),
                url,
            };
            eventBus.publish('market:connected', connected);
            logger.info(m('ok', `[MarketGateway] connected${url ? ` to ${url}` : ''}`));

            if (payload.subscriptions?.length) {
                await this.subscribeTopics(payload.meta, payload.subscriptions);
            }
        } catch (err) {
            this.publishError(payload.meta, 'connect', err);
        }
    }

    private async handleDisconnect(payload: MarketDisconnectRequest): Promise<void> {
        try {
            await this.wsClient.disconnect();
            this.publishDisconnected(payload.meta, payload.reason);
        } catch (err) {
            this.publishError(payload.meta, 'disconnect', err);
        }
    }

    private async handleSubscribe(payload: MarketSubscribeRequest): Promise<void> {
        try {
            await this.subscribeTopics(payload.meta, payload.topics);
        } catch (err) {
            this.publishError(payload.meta, 'subscribe', err);
        }
    }

    private async subscribeTopics(parentMeta: EventMeta, topics: string[]): Promise<void> {
        for (const topic of topics) {
            if (topic.startsWith('tickers.')) {
                const symbol = topic.split('.')[1];
                if (symbol) {
                    this.wsClient.subscribeTicker(symbol);
                    logger.info(m('connect', `[MarketGateway] subscribe ${topic}`));
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
        eventBus.publish('market:disconnected', payload);
    }

    private publishError(meta: EventMeta, phase: MarketErrorEvent['phase'], err: unknown): void {
        const payload: MarketErrorEvent = {
            meta: inheritMeta(meta, 'market'),
            phase,
            message: err instanceof Error ? err.message : String(err),
            error: err,
        };
        eventBus.publish('market:error', payload);
        logger.error(m('error', `[MarketGateway] ${phase} failed: ${payload.message}`));
    }
}
