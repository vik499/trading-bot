// Импортируем библиотеку ws — это WebSocket-клиент для Node.js.
// WebSocket — основной транспорт для получения стриминговых данных от Bybit.
import WebSocket, { RawData } from 'ws';

// Импортируем наш логгер — централизованный способ логирования
// (чтобы позже легко заменить формат, вывод, уровни логов и т.д.)
import { logger } from '../../infra/logger';
import { eventBus } from '../../core/events/EventBus';
import { m } from '../../core/logMarkers';
import type { EventMeta, EventSource, TickerEvent } from '../../core/events/EventBus';

type WsClientOptions = {
    pingIntervalMs?: number;
    watchdogMs?: number;
};

type WsConnState = 'idle' | 'connecting' | 'open' | 'closing';

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


    // Текущая эпоха подключения (инкремент при каждом connect())
    private connEpoch = 0;

    constructor(url: string, opts: WsClientOptions = {}) {
        this.url = url;
        this.pingIntervalMs = opts.pingIntervalMs ?? 20_000;
        this.watchdogMs = opts.watchdogMs ?? 120_000;
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

        logger.info(m('cleanup', `[BybitWS] Cleanup: ${reason}`));
    }

    // Планировщик реконнекта с экспоненциальной задержкой + jitter
    private scheduleReconnect(reason: string): void {
        if (!this.autoReconnect) return;
        if (this.isDisconnecting) return;
        if (this.isConnecting) return;
        if (this.reconnectTimer) return;

        this.reconnectAttempts += 1;

        // 1s, 2s, 4s, 8s ... максимум 30s
        const base = Math.min(30_000, 1_000 * Math.pow(2, this.reconnectAttempts - 1));
        const jitter = Math.floor(Math.random() * 500); // 0..500ms
        const delay = base + jitter;

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

    private markAlive(reason: 'open' | 'message' | 'pong_native' | 'pong_parsed'): void {
        const now = Date.now();
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
        this.lastPingAt = Date.now();
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

                if (isClean || isForcedTest) logger.info(m('socket', msg));
                else logger.error(m('error', msg));

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
                logger.error(m('error', `[BybitWS] Ошибка WebSocket: ${toErrorMessage(err)}`));

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

            // Helper to build meta for events, type-safe and future-proof
            const makeMeta = (source: EventSource): EventMeta => ({
                source,
                ts: Date.now(),
            });

            socket.on('message', (data: RawData) => {
                this.markAlive('message');
                const text = data.toString();

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

                    const tickerEvent: TickerEvent = {
                        symbol: String(ticker.symbol ?? parsed.topic.split('.')[1]),
                        lastPrice: toStr(ticker.lastPrice ?? ticker.last_price),
                        markPrice: toStr(ticker.markPrice ?? ticker.mark_price),
                        indexPrice: toStr(ticker.indexPrice ?? ticker.index_price),
                        price24hPcnt: toStr(ticker.price24hPcnt ?? ticker.price_24h_pcnt),
                        highPrice24h: toStr(ticker.highPrice24h ?? ticker.high_price_24h),
                        lowPrice24h: toStr(ticker.lowPrice24h ?? ticker.low_price_24h),
                        volume24h: toStr(ticker.volume24h ?? ticker.volume_24h),
                        turnover24h: toStr(ticker.turnover24h ?? ticker.turnover_24h),
                        meta: makeMeta('market'),
                    };

                    // MVP: без lastPrice смысла нет
                    if (!tickerEvent.lastPrice) return;

                    // Общаемся с системой строго через EventBus
                    eventBus.publish('market:ticker', tickerEvent);

                    return;
                }
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

            const now = Date.now();
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