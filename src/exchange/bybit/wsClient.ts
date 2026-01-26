// Импортируем библиотеку ws — это WebSocket-клиент для Node.js.
// WebSocket — основной транспорт для получения стриминговых данных от Bybit.
import WebSocket, { RawData } from 'ws';

// Импортируем наш логгер — централизованный способ логирования
// (чтобы позже легко заменить формат, вывод, уровни логов и т.д.)
import { logger } from '../../infra/logger';
import { eventBus, nowMs, type EventBus } from '../../core/events/EventBus';
import { m } from '../../core/logMarkers';
import type {
    EventMeta,
    EventSource,
    KlineEvent,
    KlineInterval,
    LiquidationEvent,
    MarketType,
    VenueId,
    OrderbookL2DeltaEvent,
    OrderbookL2SnapshotEvent,
    OrderbookLevel,
    TickerEvent,
    TradeEvent,
} from '../../core/events/EventBus';
import {
    mapCandleRaw,
    mapIndexPriceRaw,
    mapLiquidationRaw,
    mapMarkPriceRaw,
    mapOrderbookDeltaRaw,
    mapOrderbookSnapshotRaw,
    mapTradeRaw,
} from '../normalizers/rawAdapters';

type WsClientOptions = {
    pingIntervalMs?: number;
    watchdogMs?: number;
    now?: () => number;
    streamId?: string;
    marketType?: MarketType;
    supportsLiquidations?: boolean;
    eventBus?: EventBus;
};

type WsConnState = 'idle' | 'connecting' | 'open' | 'closing';

const DEFAULT_STREAM_ID = 'bybit.public.linear.v5';
const DEFAULT_ORDERBOOK_DEPTH = 50;

function toErrorMessage(err: unknown): string {
    if (err instanceof Error) return err.message;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
}

// ============================================================================
// BybitPublicWsClient
// ----------------------------------------------------------------------------
// Этот класс отвечает ТОЛЬКО за:
// - подключение к публичному WebSocket Bybit
// - поддержание соединения (heartbeat)
// - отправку подписок
// - приём входящих сообщений
//
// Он НЕ:
// - принимает торговые решения
// - не считает индикаторы
// - не знает о стратегии
// - не работает с ордерами
//
// Это строго Exchange/Data Layer.
// ============================================================================

export class BybitPublicWsClient {
    // Текущее WebSocket-соединение.
    private socket: WebSocket | null = null;

    // Таймер heartbeat (ping) + эпоха соединения для защиты от гонок
    private heartbeatTimer: NodeJS.Timeout | null = null;
    private heartbeatEpoch: number | null = null;

    // Текущее состояние соединения (для читаемости и защиты от гонок)
    private state: WsConnState = 'idle';

    // Был ли успешный open() для текущего сокета
    private hasOpened = false;

    // Запоминаем подписки, чтобы при reconnect пере-подписаться
    private readonly subscriptions = new Set<string>();

    // URL WebSocket-сервера Bybit.
    private readonly url: string;

    // Настройки интервалов (настраиваемые для тестов)
    private readonly pingIntervalMs: number;
    private readonly watchdogMs: number;

    // Флаг: connect() запущен и ждём 'open'
    private isConnecting = false;

    // Флаг: ручной disconnect (чтобы не запускать авто-reconnect)
    private isDisconnecting = false;

    // Таймаут подключения (если open не пришёл)
    private connectTimeout: NodeJS.Timeout | null = null;

    // connect() должен быть идемпотентным: повторные вызовы ждут один и тот же Promise
    private connectPromise: Promise<void> | null = null;
    private disconnectPromise: Promise<void> | null = null;
    private disconnectResolver: (() => void) | null = null;

    // Авто-реконнект (для 24/7 работы)
    private autoReconnect = true;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private reconnectAttempts = 0;

    // Контроль живости соединения по входящей активности
    private lastActivityAt = 0;
    private lastPongAt = 0;
    private lastPingAt = 0;
    private messageCount = 0;
    private connectedAt = 0;
    private lastErrorMessage?: string;
    private lastErrorAt?: number;
    private lastErrorEpoch?: number;
    private readonly orderbookSeq = new Map<string, number>();

    private readonly now: () => number;
    private readonly streamId: string;
    private readonly marketType?: MarketType;
    private readonly supportsLiquidations: boolean;
    private readonly bus: EventBus;
    private readonly venue: VenueId = 'bybit';

    // Текущая эпоха подключения (инкремент при каждом connect())
    private connEpoch = 0;

    constructor(url: string, opts: WsClientOptions = {}) {
        this.url = url;
        this.pingIntervalMs = opts.pingIntervalMs ?? 30_000;
        this.watchdogMs = opts.watchdogMs ?? 120_000;
        this.now = opts.now ?? nowMs;
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.marketType = opts.marketType;
        this.supportsLiquidations = opts.supportsLiquidations ?? false;
        this.bus = opts.eventBus ?? eventBus;
    }

    // ------------------------------------------------------------------------
    // cleanupSocket
    // ------------------------------------------------------------------------
    private cleanupSocket(reason: string): void {
        if (this.connectTimeout) {
            clearTimeout(this.connectTimeout);
            this.connectTimeout = null;
        }

        // Всегда стопаем heartbeat
        this.stopHeartbeat();

        const socket = this.socket;
        if (socket) {
            try {
                socket.removeAllListeners();

                // Аккуратное закрытие без исключений
                if (socket.readyState === WebSocket.CONNECTING) {
                    try {
                        socket.close();
                    } catch {
                        // ignore
                    }
                } else if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CLOSING) {
                    try {
                        socket.close();
                    } catch {
                        // ignore
                    }
                }
            } catch {
                // ignore
            }
        }

        this.socket = null;
        this.heartbeatEpoch = null;
        this.isConnecting = false;
        this.connectPromise = null;
        this.hasOpened = false;

        // Если мы не в ручном disconnect — возвращаем авто-реконнект в нормальный режим
        // (при ручном disconnect autoReconnect отключаем выше, в disconnect())
        if (!this.isDisconnecting) {
            this.autoReconnect = true;
        }

        if (this.isDisconnecting && this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        this.state = 'idle';
        this.lastActivityAt = 0;
        this.lastPingAt = 0;
        this.lastPongAt = 0;
        this.messageCount = 0;
        this.connectedAt = 0;
        this.lastErrorMessage = undefined;
        this.lastErrorAt = undefined;
        this.lastErrorEpoch = undefined;

        logger.info(m('cleanup', `[BybitWS] Cleanup: ${reason}`));
    }

    // Планировщик реконнекта с экспоненциальной задержкой + jitter
    private scheduleReconnect(reason: string): void {
        if (!this.autoReconnect) return;
        if (this.isDisconnecting) return;
        if (this.isConnecting) return;
        if (this.reconnectTimer) return;

        this.reconnectAttempts += 1;

        const delay = computeBybitReconnectDelayMs(this.reconnectAttempts);

        logger.warn(m('warn', `[BybitWS] Планируем reconnect через ${delay}мс (attempt=${this.reconnectAttempts}, reason=${reason})`));

        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = null;
            try {
                await this.connect();
                this.reconnectAttempts = 0;
            } catch (err) {
                const msg = err instanceof Error ? err.message : String(err);
                logger.warn(`[BybitWS] Reconnect не удался: ${msg}`);
                this.scheduleReconnect('reconnect failed');
            }
        }, delay);
    }

    private requestResync(symbol: string, reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown', details?: Record<string, unknown>): void {
        this.bus.publish('market:resync_requested', {
            venue: this.venue,
            symbol,
            channel: 'orderbook',
            reason,
            streamId: this.streamId,
            details,
            meta: this.makeMeta('market', this.now()),
        });
    }

    private markAlive(reason: 'open' | 'message' | 'pong_native' | 'pong_parsed'): void {
        const now = this.now();
        this.lastActivityAt = now;
        if (reason === 'open') {
            logger.debug(m('heartbeat', '[BybitWS] alive (open)'));
            return;
        }
        if (reason === 'message') return;
        if (reason === 'pong_native') {
            this.lastPongAt = now;
            logger.debug(m('heartbeat', '[BybitWS] pong (native)'));
            return;
        }
        if (reason === 'pong_parsed') {
            this.lastPongAt = now;
            logger.debug(m('heartbeat', '[BybitWS] pong (parsed)'));
        }
    }

    private markPing(): void {
        this.lastPingAt = this.now();
    }

    private formatMs(ms: number): string {
        return `${(ms / 1000).toFixed(1)}s`;
    }

    // cleanup вызываем строго один раз на закрытие сокета
    private cleanupDone = false;
    private cleanupOnce(reason: string): void {
        if (this.cleanupDone) return;
        this.cleanupDone = true;
        this.cleanupSocket(reason);
    }

    // ------------------------------------------------------------------------
    // connect
    // ------------------------------------------------------------------------
    async connect(): Promise<void> {
        // Идемпотентность: если connect уже идёт — возвращаем тот же Promise
        if (this.isConnecting) {
            if (this.connectPromise) return this.connectPromise;
            return Promise.reject(new Error('BybitWS connect already in progress'));
        }

        // Если соединение уже открыто — повторно не подключаемся
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            logger.info('[BybitWS] Уже подключены');
            return;
        }

        // Если был запланирован reconnect — отменяем, так как сейчас подключаемся явно
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        this.isConnecting = true;
        this.isDisconnecting = false;
        this.state = 'connecting';
        this.hasOpened = false;
        this.connEpoch += 1;
        const epoch = this.connEpoch;

        logger.info(m('connect', `[BybitWS] Подключение к ${this.url}...`));

        // Возвращаем (и сохраняем) Promise, который будет завершён при open / error
        this.connectPromise = new Promise((resolve, reject) => {
            let settled = false;

            const settle = (kind: 'resolve' | 'reject', err?: Error) => {
                if (settled) return;
                settled = true;
                this.connectPromise = null;
                if (kind === 'resolve') resolve();
                else reject(err ?? new Error('BybitWS connect failed'));
            };

            const safeResolve = () => settle('resolve');
            const safeReject = (e: Error) => settle('reject', e);

            this.cleanupDone = false;
            const socket = new WebSocket(this.url);
            this.socket = socket;

            socket.on('pong', () => {
                this.markAlive('pong_native');
            });

            // Таймаут подключения
            const connectTimeoutMs = 15_000;

            this.connectTimeout = setTimeout(() => {
                logger.error(m('timeout', `[BybitWS] Таймаут подключения (${connectTimeoutMs}мс)`));

                try {
                    // Важно: в CONNECTING terminate() иногда кидает исключение (как ты уже видел).
                    // Для таймаута достаточно close().
                    socket.close();
                } catch {
                    // ignore
                }

                this.cleanupOnce('connect timeout');
                safeReject(new Error('BybitWS connect timeout'));
            }, connectTimeoutMs);

            const clearConnectTimeout = () => {
                if (this.connectTimeout) {
                    clearTimeout(this.connectTimeout);
                    this.connectTimeout = null;
                }
            };

            const onOpen = () => {
                logger.info(m('ok', '[BybitWS] Соединение установлено.'));

                clearConnectTimeout();

                this.isConnecting = false;
                this.state = 'open';
                this.hasOpened = true;
                this.connectedAt = this.now();
                this.messageCount = 0;
                this.lastErrorMessage = undefined;
                this.lastErrorAt = undefined;
                this.lastErrorEpoch = undefined;

                this.markAlive('open'); // считаем, что соединение живое с момента open
                this.startHeartbeat(epoch);

                // Авто-повтор подписок после reconnect
                if (this.subscriptions.size > 0) {
                    for (const topic of this.subscriptions) {
                        this.sendJson({ op: 'subscribe', args: [topic] });
                        logger.info(m('connect', `[BybitWS] Re-subscribe: ${topic}`));
                    }
                }

                safeResolve();
            };

            const onClose = (code: number, reason: Buffer | string) => {
                const reasonText = typeof reason === 'string' ? reason : reason?.toString?.() || '';
                const msg = `[BybitWS] Соединение закрыто. Код=${code}${reasonText ? `, причина=${reasonText}` : ''}`;

                const isManual = this.isDisconnecting;
                const isClean = code === 1000 || (isManual && (code === 1000 || code === 1005));
                const isForcedTest = code === 1001 && reasonText.toLowerCase().includes('forced');

                const lifetimeMs = this.connectedAt > 0 ? Math.max(0, this.now() - this.connectedAt) : 0;
                const lastErrInfo =
                    this.lastErrorEpoch === this.connEpoch && this.lastErrorMessage
                        ? ` lastError="${this.lastErrorMessage}"` + (this.lastErrorAt ? ` lastErrorAt=${this.lastErrorAt}` : '')
                        : '';
                const statsLine = `[BybitWS] Close stats: lifetimeMs=${lifetimeMs} messages=${this.messageCount}${lastErrInfo}`;

                if (isClean || isForcedTest) {
                    logger.info(m('socket', msg));
                    logger.info(m('socket', statsLine));
                } else {
                    logger.error(m('error', msg));
                    logger.warn(m('warn', statsLine));
                }

                clearConnectTimeout();

                const wasConnecting = this.state === 'connecting' && !this.hasOpened;
                const wasDisconnecting = this.isDisconnecting;

                this.state = 'closing';
                this.cleanupOnce('close');

                // Если закрылись до open (timeout/банальная сеть) — это ошибка connect()
                if (wasConnecting && !wasDisconnecting) {
                    safeReject(new Error(`BybitWS closed before open (code=${code})`));
                }

                // Если это не ручной disconnect — пробуем восстановиться
                if (!wasDisconnecting) {
                    this.scheduleReconnect(`close code=${code}`);
                }

                if (this.disconnectResolver) {
                    const resolve = this.disconnectResolver;
                    this.disconnectResolver = null;
                    resolve();
                }
            };

            const onError = (err: Error) => {
                const msg = toErrorMessage(err);
                this.lastErrorMessage = msg;
                this.lastErrorAt = this.now();
                this.lastErrorEpoch = this.connEpoch;
                logger.error(m('error', `[BybitWS] Ошибка WebSocket: ${msg}`));

                clearConnectTimeout();

                const wasDisconnecting = this.isDisconnecting;
                const wasConnecting = this.state === 'connecting' && !this.hasOpened;

                this.cleanupOnce('error');

                // Если ошибка случилась во время первичного connect — это ошибка connect()
                if (wasConnecting) {
                    safeReject(err);
                }

                if (!wasDisconnecting) {
                    this.scheduleReconnect('error');
                }
            };

            socket.once('open', onOpen);
            socket.on('error', onError);
            socket.on('close', onClose);
            socket.on('ping', () => {
                // ws сам отвечает pong, но фиксируем входящий ping как активность.
                this.markAlive('pong_native');
            });

            socket.on('message', (data: RawData) => {
                this.handleMessage(data.toString());
            });
        });

        return this.connectPromise;
    }

    // ------------------------------------------------------------------------
    // disconnect
    // ------------------------------------------------------------------------
    async disconnect(): Promise<void> {
        if (this.disconnectPromise) return this.disconnectPromise;

        if (!this.socket) return;

        const socket = this.socket;
        this.isDisconnecting = true;
        this.state = 'closing';

        // При ручном disconnect запрещаем авто-реконнект
        this.autoReconnect = false;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        logger.info('[BybitWS] Отключаемся вручную...');

        this.stopHeartbeat();

        if (socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) {
            const done = Promise.resolve().finally(() => {
                this.disconnectResolver = null;
                this.disconnectPromise = null;
                this.isDisconnecting = false;
                this.state = 'idle';
                this.lastActivityAt = 0;
                this.lastPingAt = 0;
                this.lastPongAt = 0;
            });
            this.disconnectPromise = done;
            return done;
        }

        const closeTimeoutMs = 2_000;
        const inFlight = new Promise<void>((resolve) => {
            let finished = false;
            const finish = () => {
                if (finished) return;
                finished = true;
                clearTimeout(timeout);
                if (this.disconnectResolver === finish) this.disconnectResolver = null;
                resolve();
            };

            this.disconnectResolver = finish;

            const timeout = setTimeout(() => {
                try {
                    if (socket.readyState === WebSocket.CONNECTING) {
                        socket.close();
                    } else {
                        socket.terminate();
                    }
                } catch {
                    // ignore
                }
                finish();
            }, closeTimeoutMs);

            socket.once('close', () => {
                finish();
            });

            try {
                socket.close();
            } catch {
                finish();
            }
        });

        this.disconnectPromise = inFlight.finally(() => {
            this.disconnectResolver = null;
            this.disconnectPromise = null;
            this.isDisconnecting = false;
            this.state = 'idle';
            this.lastActivityAt = 0;
            this.lastPingAt = 0;
            this.lastPongAt = 0;
        });

        return this.disconnectPromise;
    }

    // ------------------------------------------------------------------------
    // subscribeTicker
    // ------------------------------------------------------------------------
    subscribeTicker(symbol: string): void {
        const topic = `tickers.${symbol}`;
        this.subscriptions.add(topic);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.info(m('ws', `[BybitWS] WebSocket не открыт — подписка сохранена и будет отправлена при подключении: ${topic}`));
            return;
        }

        this.sendJson({ op: 'subscribe', args: [topic] });
        logger.info(m('connect', `[BybitWS] Подписка отправлена: ${topic}`));
    }

    // ------------------------------------------------------------------------
    // subscribeTrades
    // ------------------------------------------------------------------------
    subscribeTrades(symbol: string): void {
        const topic = `publicTrade.${symbol}`;
        this.subscriptions.add(topic);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.info(m('ws', `[BybitWS] WebSocket не открыт — подписка сохранена и будет отправлена при подключении: ${topic}`));
            return;
        }

        this.sendJson({ op: 'subscribe', args: [topic] });
        logger.info(m('connect', `[BybitWS] Подписка отправлена: ${topic}`));
    }

    // ------------------------------------------------------------------------
    // subscribeOrderbook
    // ------------------------------------------------------------------------
    subscribeOrderbook(symbol: string, depth = DEFAULT_ORDERBOOK_DEPTH): void {
        const topic = `orderbook.${depth}.${symbol}`;
        this.subscriptions.add(topic);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.info(m('ws', `[BybitWS] WebSocket не открыт — подписка сохранена и будет отправлена при подключении: ${topic}`));
            return;
        }

        this.sendJson({ op: 'subscribe', args: [topic] });
        logger.info(m('connect', `[BybitWS] Подписка отправлена: ${topic}`));
    }

    // ------------------------------------------------------------------------
    // subscribeKlines
    // ------------------------------------------------------------------------
    subscribeKlines(symbol: string, interval: KlineInterval): void {
        const topic = `kline.${toBybitKlineInterval(interval)}.${symbol}`;
        this.subscriptions.add(topic);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.info(m('ws', `[BybitWS] WebSocket не открыт — подписка сохранена и будет отправлена при подключении: ${topic}`));
            return;
        }

        this.sendJson({ op: 'subscribe', args: [topic] });
        logger.info(m('connect', `[BybitWS] Подписка отправлена: ${topic}`));
    }

    // ------------------------------------------------------------------------
    // subscribeLiquidations
    // ------------------------------------------------------------------------
    subscribeLiquidations(symbol: string): void {
        if (!this.supportsLiquidations) return;
        const topic = `allLiquidation.${symbol}`;
        this.subscriptions.add(topic);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.info(m('ws', `[BybitWS] WebSocket не открыт — подписка сохранена и будет отправлена при подключении: ${topic}`));
            return;
        }

        this.sendJson({ op: 'subscribe', args: [topic] });
        logger.info(m('connect', `[BybitWS] Подписка отправлена: ${topic}`));
    }

    private makeMeta(source: EventSource, ts?: number): EventMeta {
        return {
            source,
            ts: ts ?? this.now(),
        };
    }

    private handleMessage(text: string): void {
        this.messageCount += 1;
        this.markAlive('message');

        let parsed: any = null;
        try {
            parsed = JSON.parse(text);
        } catch {
            return;
        }

        // pong от Bybit = признак живого соединения
        if (parsed?.op === 'pong') {
            this.markAlive('pong_parsed');
            return;
        }

        // ping не логируем
        if (parsed?.op === 'ping') {
            return;
        }

        // Системные сообщения (ack на subscribe и т.п.)
        if (typeof parsed?.success === 'boolean' || parsed?.ret_msg) {
            const preview = text.length > 300 ? `${text.slice(0, 300)}…` : text;
            logger.info(m('ws', `[BybitWS] Системное сообщение: ${preview}`));
            return;
        }

        // Рыночные данные: тикер
        if (typeof parsed?.topic === 'string' && parsed.topic.startsWith('tickers.')) {
            const raw = parsed?.data;
            const ticker = Array.isArray(raw) ? raw[0] : raw;

            if (!ticker || typeof ticker !== 'object') return;

            const toStr = (v: unknown): string | undefined => {
                if (v === undefined || v === null) return undefined;
                const s = String(v);
                return s === '' ? undefined : s;
            };

            const exchangeTs = toNumber(ticker.ts ?? ticker.timestamp ?? ticker.time ?? parsed.ts);
            const recvTs = this.now();
            const tickerEvent: TickerEvent = {
                symbol: String(ticker.symbol ?? parsed.topic.split('.')[1]),
                streamId: this.streamId,
                marketType: this.marketType,
                lastPrice: toStr(ticker.lastPrice ?? ticker.last_price),
                markPrice: toStr(ticker.markPrice ?? ticker.mark_price),
                indexPrice: toStr(ticker.indexPrice ?? ticker.index_price),
                price24hPcnt: toStr(ticker.price24hPcnt ?? ticker.price_24h_pcnt),
                highPrice24h: toStr(ticker.highPrice24h ?? ticker.high_price_24h),
                lowPrice24h: toStr(ticker.lowPrice24h ?? ticker.low_price_24h),
                volume24h: toStr(ticker.volume24h ?? ticker.volume_24h),
                turnover24h: toStr(ticker.turnover24h ?? ticker.turnover_24h),
                exchangeTs,
                meta: this.makeMeta('market', exchangeTs),
            };

            // MVP: без lastPrice смысла нет
            if (!tickerEvent.lastPrice) return;

            // Общаемся с системой строго через EventBus
            this.bus.publish('market:ticker', tickerEvent);

            if (exchangeTs !== undefined) {
                if (tickerEvent.markPrice) {
                    this.bus.publish(
                        'market:mark_price_raw',
                        mapMarkPriceRaw(this.venue, tickerEvent.symbol, tickerEvent.markPrice, exchangeTs, recvTs, this.marketType)
                    );
                }
                if (tickerEvent.indexPrice) {
                    this.bus.publish(
                        'market:index_price_raw',
                        mapIndexPriceRaw(this.venue, tickerEvent.symbol, tickerEvent.indexPrice, exchangeTs, recvTs, this.marketType)
                    );
                }
            }

            return;
        }

        // Рыночные данные: сделки (trades / publicTrade)
        if (typeof parsed?.topic === 'string' && (parsed.topic.startsWith('publicTrade.') || parsed.topic.startsWith('trades.'))) {
            const raw = parsed?.data;
            if (!Array.isArray(raw)) return;

            for (const item of raw) {
                if (!item || typeof item !== 'object') continue;
                const obj = item as Record<string, unknown>;
                const symbol = String(obj.s ?? obj.symbol ?? parsed.topic.split('.')[1] ?? 'n/a');
                const sideRaw = obj.S ?? obj.side;
                const side = sideRaw === 'Buy' || sideRaw === 'Sell' ? sideRaw : undefined;
                const price = toNumber(obj.p ?? obj.price);
                const size = toNumber(obj.v ?? obj.size ?? obj.qty);
                const tradeTs = toNumber(obj.T ?? obj.tradeTime ?? obj.tradeTs ?? parsed.ts);
                if (!side || price === undefined || size === undefined || tradeTs === undefined) continue;

                const tradeIdRaw = obj.i ?? obj.tradeId ?? obj.execId;
                const tradeId = tradeIdRaw !== undefined ? String(tradeIdRaw) : undefined;
                const recvTs = this.now();
                const meta = this.makeMeta('market', tradeTs);
                const tradeEvent: TradeEvent = {
                    symbol,
                    streamId: this.streamId,
                    tradeId,
                    side,
                    price,
                    size,
                    tradeTs,
                    exchangeTs: tradeTs,
                    marketType: this.marketType,
                    meta,
                };
                this.bus.publish('market:trade', tradeEvent);

                const rawEvt = mapTradeRaw(this.venue, symbol, obj, tradeTs, recvTs, this.marketType);
                if (rawEvt) this.bus.publish('market:trade_raw', rawEvt);
            }
            return;
        }

        // Рыночные данные: стакан L2 (orderbook)
        if (typeof parsed?.topic === 'string' && parsed.topic.startsWith('orderbook.')) {
            const raw = parsed?.data;
            const payload = Array.isArray(raw) ? raw[0] : raw;
            if (!payload || typeof payload !== 'object') return;
            const obj = payload as Record<string, unknown>;
            const symbol = String(obj.s ?? obj.symbol ?? parsed.topic.split('.')[2] ?? parsed.topic.split('.')[1] ?? 'n/a');
            const updateId = toNumber(obj.u ?? obj.updateId ?? obj.seq);
            if (updateId === undefined) return;

            const bids = parseLevels(obj.b ?? obj.bids);
            const asks = parseLevels(obj.a ?? obj.asks);
            const exchangeTs = toNumber(obj.ts ?? obj.T ?? parsed.ts);
            const recvTs = this.now();
            const meta = this.makeMeta('market', exchangeTs);
            const isSnapshot = String(parsed?.type ?? obj.type ?? '').toLowerCase() === 'snapshot';

            const lastSeq = this.orderbookSeq.get(symbol);
            if (isSnapshot) {
                this.orderbookSeq.set(symbol, updateId);
            } else if (lastSeq === undefined) {
                this.requestResync(symbol, 'snapshot_missing', { updateId });
                return;
            } else if (updateId > lastSeq + 1) {
                this.requestResync(symbol, 'gap', { lastSeq, updateId });
                this.orderbookSeq.set(symbol, updateId);
                return;
            } else if (updateId <= lastSeq) {
                return;
            } else {
                this.orderbookSeq.set(symbol, updateId);
            }

            if (isSnapshot) {
                const snapshot: OrderbookL2SnapshotEvent = {
                    symbol,
                    streamId: this.streamId,
                    updateId,
                    exchangeTs,
                    marketType: this.marketType,
                    bids,
                    asks,
                    meta,
                };
                this.bus.publish('market:orderbook_l2_snapshot', snapshot);
                if (exchangeTs !== undefined) {
                    this.bus.publish(
                        'market:orderbook_snapshot_raw',
                        mapOrderbookSnapshotRaw(this.venue, symbol, obj.b ?? obj.bids, obj.a ?? obj.asks, exchangeTs, recvTs, updateId, this.marketType)
                    );
                }
            } else {
                const delta: OrderbookL2DeltaEvent = {
                    symbol,
                    streamId: this.streamId,
                    updateId,
                    exchangeTs,
                    marketType: this.marketType,
                    bids,
                    asks,
                    meta,
                };
                this.bus.publish('market:orderbook_l2_delta', delta);
                if (exchangeTs !== undefined) {
                    this.bus.publish(
                        'market:orderbook_delta_raw',
                        mapOrderbookDeltaRaw(this.venue, symbol, obj.b ?? obj.bids, obj.a ?? obj.asks, exchangeTs, recvTs, updateId, undefined, undefined, this.marketType)
                    );
                }
            }
            return;
        }

        // Рыночные данные: свечи (kline)
        if (typeof parsed?.topic === 'string' && parsed.topic.startsWith('kline.')) {
            const raw = parsed?.data;
            const rows = Array.isArray(raw) ? raw : raw ? [raw] : [];
            if (!rows.length) return;

            const parts = parsed.topic.split('.');
            const interval = fromBybitKlineInterval(parts[1]);
            const symbol = String(parts[2] ?? 'n/a');
            const tf = interval ? intervalToTf(interval) : undefined;

            for (const row of rows) {
                if (!row || typeof row !== 'object') continue;
                const obj = row as Record<string, unknown>;
                const startTs = toNumber(obj.start ?? obj.startTime ?? obj.start_ts ?? obj.ts);
                const endTs = toNumber(obj.end ?? obj.endTime ?? obj.end_ts);
                const open = toNumber(obj.open);
                const high = toNumber(obj.high);
                const low = toNumber(obj.low);
                const close = toNumber(obj.close);
                const volume = toNumber(obj.volume ?? obj.vol);
                const confirmed = Boolean(obj.confirm ?? obj.confirmed ?? obj.isConfirm);

                if (!confirmed) continue;
                if (
                    interval === undefined ||
                    startTs === undefined ||
                    endTs === undefined ||
                    open === undefined ||
                    high === undefined ||
                    low === undefined ||
                    close === undefined ||
                    volume === undefined
                ) {
                    continue;
                }

                const event: KlineEvent = {
                    symbol,
                    interval,
                    tf: tf ?? interval,
                    startTs,
                    endTs,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    streamId: this.streamId,
                    marketType: this.marketType,
                    meta: this.makeMeta('market', endTs),
                };
                this.bus.publish('market:kline', event);

                const rawEvt = mapCandleRaw(
                    this.venue,
                    symbol,
                    tf ?? interval,
                    startTs,
                    endTs,
                    obj.open,
                    obj.high,
                    obj.low,
                    obj.close,
                    obj.volume ?? obj.vol,
                    true,
                    endTs,
                    this.now(),
                    this.marketType
                );
                this.bus.publish('market:candle_raw', rawEvt);
            }
            return;
        }

        // Рыночные данные: ликвидации (allLiquidation)
        if (
            this.supportsLiquidations &&
            typeof parsed?.topic === 'string' &&
            parsed.topic.startsWith('allLiquidation.')
        ) {
            const raw = parsed?.data;
            const rows = Array.isArray(raw) ? raw : raw ? [raw] : [];
            if (!rows.length) return;

            for (const row of rows) {
                if (!row || typeof row !== 'object') continue;
                const obj = row as Record<string, unknown>;
                const symbol = String(obj.symbol ?? obj.s ?? parsed.topic.split('.')[1] ?? 'n/a');
                const sideRaw = obj.side ?? obj.S;
                const side = sideRaw === 'Buy' || sideRaw === 'Sell' ? sideRaw : undefined;
                const price = toNumber(obj.price ?? obj.p);
                const size = toNumber(obj.qty ?? obj.size ?? obj.q ?? obj.v);
                const exchangeTs = toNumber(obj.ts ?? obj.T ?? parsed.ts);

                const payload: LiquidationEvent = {
                    symbol,
                    streamId: this.streamId,
                    side,
                    price,
                    size,
                    notionalUsd: price !== undefined && size !== undefined ? price * size : undefined,
                    exchangeTs,
                    marketType: this.marketType,
                    meta: this.makeMeta('market', exchangeTs ?? parsed.ts),
                };
                this.bus.publish('market:liquidation', payload);

                if (exchangeTs !== undefined) {
                    const rawEvt = mapLiquidationRaw(
                        this.venue,
                        symbol,
                        exchangeTs,
                        this.now(),
                        { side: side?.toLowerCase(), price, size, notionalUsd: payload.notionalUsd },
                        this.marketType
                    );
                    this.bus.publish('market:liquidation_raw', rawEvt);
                }
            }
            return;
        }
    }

    // ------------------------------------------------------------------------
    // sendJson
    // ------------------------------------------------------------------------
    private sendJson(payload: unknown, opts: { silent?: boolean } = {}): void {
        const silent = Boolean(opts.silent);

        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            if (!silent) logger.error('[BybitWS] Нельзя отправить сообщение: WebSocket не открыт');
            return;
        }

        try {
            this.socket.send(JSON.stringify(payload));
        } catch (err) {
            if (!silent) logger.error(`[BybitWS] Ошибка отправки сообщения: ${toErrorMessage(err)}`);
        }
    }

    // ------------------------------------------------------------------------
    // startHeartbeat
    // ------------------------------------------------------------------------
    private startHeartbeat(epoch: number): void {
        if (this.heartbeatTimer) return;

        this.heartbeatEpoch = epoch;

        this.heartbeatTimer = setInterval(() => {
            // Защита от гонок: если эпоха изменилась — таймер устарел
            if (this.heartbeatEpoch !== epoch) {
                this.stopHeartbeat();
                return;
            }

            if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;

            const now = this.now();
            const silence = this.lastActivityAt > 0 ? now - this.lastActivityAt : 0;

            // Живость определяем по любой входящей активности, не только по pong
            if (this.lastActivityAt > 0 && silence > this.watchdogMs) {
                logger.warn(
                    `[BybitWS] Нет входящей активности ${this.formatMs(silence)} — считаем соединение подвисшим, делаем reconnect`
                );
                try {
                    this.socket?.close();
                } catch {
                    // ignore
                }
                return;
            }

            this.markPing();
            // ping делаем тихо: если сокет уже закрыт, это не “ошибка”, это нормальное состояние при реконнекте
            this.sendJson({ op: 'ping' }, { silent: true });
        }, this.pingIntervalMs);

        logger.info(
            m('heartbeat', `[BybitWS] Heartbeat запущен (ping каждые ${this.pingIntervalMs}мс, watchdog ${this.watchdogMs}мс по входящим данным)`)
        );
    }

    // ------------------------------------------------------------------------
    // stopHeartbeat
    // ------------------------------------------------------------------------
    private stopHeartbeat(): void {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
        this.heartbeatEpoch = null;
    }

    isAlive(): boolean {
        return Boolean(this.socket && this.socket.readyState === WebSocket.OPEN);
    }

    // ------------------------------------------------------------------------
    // getStatus
    // ------------------------------------------------------------------------
    getStatus(): {
        state: WsConnState;
        hasSocket: boolean;
        lastPongAt: number;
        lastPingAt: number;
        lastActivityAt: number;
        reconnectAttempts: number;
    } {
        return {
            state: this.state,
            hasSocket: Boolean(this.socket),
            lastPongAt: this.lastPongAt,
            lastPingAt: this.lastPingAt,
            lastActivityAt: this.lastActivityAt,
            reconnectAttempts: this.reconnectAttempts,
        };
    }
}

function toNumber(value: unknown): number | undefined {
    if (typeof value === 'number') return Number.isFinite(value) ? value : undefined;
    if (typeof value === 'string' && value !== '') {
        const num = Number(value);
        return Number.isFinite(num) ? num : undefined;
    }
    return undefined;
}

function parseLevels(raw: unknown): OrderbookLevel[] {
    if (!Array.isArray(raw)) return [];
    const levels: OrderbookLevel[] = [];
    for (const entry of raw) {
        if (Array.isArray(entry)) {
            const price = toNumber(entry[0]);
            const size = toNumber(entry[1]);
            if (price !== undefined && size !== undefined) {
                levels.push({ price, size });
            }
            continue;
        }
        if (!entry || typeof entry !== 'object') continue;
        const obj = entry as Record<string, unknown>;
        const price = toNumber(obj.price ?? obj.p);
        const size = toNumber(obj.size ?? obj.s ?? obj.q);
        if (price !== undefined && size !== undefined) {
            levels.push({ price, size });
        }
    }
    return levels;
}

function intervalToTf(interval: KlineInterval): string {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return interval;
    if (minutes < 60) return `${minutes}m`;
    if (minutes % 1440 === 0) return `${minutes / 1440}d`;
    if (minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
}

const BYBIT_KLINE_INTERVALS: Set<KlineInterval> = new Set([
    '1',
    '3',
    '5',
    '15',
    '30',
    '60',
    '120',
    '240',
    '360',
    '720',
    '1440',
]);

function toBybitKlineInterval(interval: KlineInterval): string {
    if (interval === '1440') return 'D';
    return interval;
}

function fromBybitKlineInterval(value?: string): KlineInterval | undefined {
    if (!value) return undefined;
    const normalized = value.toUpperCase();
    if (normalized === 'D') return '1440';
    return BYBIT_KLINE_INTERVALS.has(normalized as KlineInterval) ? (normalized as KlineInterval) : undefined;
}

function stableJitterFactor(seed: string): number {
    let hash = 0;
    for (let i = 0; i < seed.length; i += 1) {
        hash = (hash * 31 + seed.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}

export function computeBybitReconnectDelayMs(attempt: number, maxMs = 30_000): number {
    const safeAttempt = Math.max(1, Math.floor(attempt));
    const base = Math.min(maxMs, 1_000 * Math.pow(2, safeAttempt - 1));
    const jitter = Math.floor(500 * stableJitterFactor(`reconnect:${safeAttempt}`));
    return base + jitter;
}
