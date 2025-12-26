// Импортируем библиотеку ws — это WebSocket-клиент для Node.js.
// WebSocket — основной транспорт для получения стриминговых данных от Bybit.
import WebSocket, { RawData } from 'ws';

// Импортируем наш логгер — централизованный способ логирования
// (чтобы позже легко заменить формат, вывод, уровни логов и т.д.)
import { logger } from '../../infra/logger';
import { eventBus } from '../../core/events/EventBus';

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
    // ------------------------------------------------------------------------
    // Текущее WebSocket-соединение.
    // null — если мы ещё не подключились или уже отключились.
    // ------------------------------------------------------------------------
    private socket: WebSocket | null = null;

    // ------------------------------------------------------------------------
    // Таймер для heartbeat (ping).
    // Если его не отправлять — Bybit может закрыть соединение.
    // ------------------------------------------------------------------------
    private heartbeatTimer: NodeJS.Timeout | null = null;

    // ------------------------------------------------------------------------
    // URL WebSocket-сервера Bybit.
    // Передаётся через конструктор, чтобы:
    // - легко переключаться между testnet / mainnet
    // - не хардкодить окружение в классе
    // ------------------------------------------------------------------------
    private readonly url: string;

    // ------------------------------------------------------------------------
    // Флаг: true, если connect() уже вызван и мы ждём событие 'open'.
    // Защищает от повторных / параллельных подключений.
    // ------------------------------------------------------------------------
    private isConnecting = false;

    // ------------------------------------------------------------------------
    // Конструктор принимает URL WebSocket сервера.
    // ------------------------------------------------------------------------
    constructor(url: string) {
        this.url = url;
    }

    // ------------------------------------------------------------------------
    // cleanupSocket
    // ------------------------------------------------------------------------
    // Универсальный метод "очистки":
    // - снимает все listeners
    // - завершает соединение
    // - останавливает heartbeat
    // - сбрасывает состояние клиента
    //
    // Используется:
    // - при ошибке
    // - при таймауте
    // - при закрытии соединения
    // - при ручном disconnect
    // ------------------------------------------------------------------------
    private cleanupSocket(reason: string): void {
        // Если сокет существует — пытаемся корректно его убить
        if (this.socket) {
            try {
                // Удаляем все подписанные обработчики событий,
                // чтобы избежать утечек памяти и дублирующих хэндлеров
                this.socket.removeAllListeners();

                // Жёстко обрываем соединение (быстрее и надёжнее, чем close)
                this.socket.terminate();
            } catch {
                // Если при очистке что-то пошло не так — игнорируем,
                // потому что мы всё равно приводим клиент в "нулевое" состояние
            }
        }

        // Останавливаем heartbeat (если он был запущен)
        this.stopHeartbeat();

        // Полностью сбрасываем состояние клиента
        this.socket = null;
        this.isConnecting = false;

        // Логируем причину очистки — полезно для отладки
        logger.info(`[BybitWS] Cleanup: ${reason}`);
    }

    // ------------------------------------------------------------------------
    // connect
    // ------------------------------------------------------------------------
    // Устанавливает соединение с WebSocket сервером Bybit.
    //
    // Возвращает Promise<void>, чтобы вызывающий код мог:
    //   await wsClient.connect()
    //
    // Promise:
    // - resolve() → когда соединение успешно открыто
    // - reject()  → если произошла ошибка или таймаут
    // ------------------------------------------------------------------------
    async connect(): Promise<void> {
        // Если подключение уже в процессе — ничего не делаем
        if (this.isConnecting) {
            logger.error('[BybitWS] connect() уже выполняется');
            return;
        }

        // Если соединение уже открыто — повторно не подключаемся
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            logger.info('[BybitWS] Уже подключены');
            return;
        }

        // Отмечаем, что процесс подключения начался
        this.isConnecting = true;
        logger.info(`[BybitWS] Подключение к ${this.url}...`);

        // Возвращаем Promise, который будет завершён при open / error
        return new Promise((resolve, reject) => {
            // Создаём новый WebSocket
            const socket = new WebSocket(this.url);

            // Сохраняем ссылку на сокет
            this.socket = socket;

            // ----------------------------------------------------------------
            // Таймаут подключения
            // ----------------------------------------------------------------
            // Если за 15 секунд мы не получили событие 'open',
            // считаем соединение зависшим и падаем с ошибкой.
            // ----------------------------------------------------------------
            const connectTimeoutMs = 15_000;
            const connectTimeout = setTimeout(() => {
                logger.error(`[BybitWS] Таймаут подключения (${connectTimeoutMs}мс)`);
                this.cleanupSocket('connect timeout');
                reject(new Error('BybitWS connect timeout'));
            }, connectTimeoutMs);

            // Вспомогательная функция очистки таймаута
            const clearConnectTimeout = () => clearTimeout(connectTimeout);

            // ----------------------------------------------------------------
            // Обработчик закрытия соединения
            // ----------------------------------------------------------------
            const onClose = (code: number, reason: Buffer) => {
                const reasonText = reason?.toString?.() || '';
                const msg = `[BybitWS] Соединение закрыто. Код=${code}${reasonText ? `, причина=${reasonText}` : ''}`;

                // 1000 = нормальное закрытие. Всё остальное чаще всего проблема/срыв.
                if (code === 1000) {
                    logger.info(msg);
                } else {
                    logger.error(msg);
                }

                // Таймаут подключения больше не нужен
                clearConnectTimeout();

                // Проверяем: было ли это закрытие во время connect()
                const wasConnecting = this.isConnecting;

                // Полная очистка состояния
                this.cleanupSocket('close');

                // Если сокет закрылся ДО open — считаем connect() неуспешным
                if (wasConnecting) {
                    reject(new Error(`BybitWS closed before open (code=${code})`));
                }
            };

            // ----------------------------------------------------------------
            // Обработчик ошибки WebSocket
            // ----------------------------------------------------------------
            const onError = (err: Error) => {
                logger.error('[BybitWS] Ошибка WebSocket:', err);

                // Убираем таймаут и чистим состояние
                clearConnectTimeout();
                this.cleanupSocket('error');

                // Проваливаем Promise connect()
                reject(err);
            };

            // ----------------------------------------------------------------
            // Обработчик успешного открытия соединения
            // ----------------------------------------------------------------
            const onOpen = () => {
                logger.info('[BybitWS] Соединение установлено.');

                // Подключение успешно — таймаут больше не нужен
                clearConnectTimeout();

                // Сбрасываем флаг подключения
                this.isConnecting = false;

                // Запускаем heartbeat (регулярные ping)
                this.startHeartbeat();

                // Сообщаем вызывающему коду, что соединение готово
                resolve();
            };

            // Подписываемся на события WebSocket
            socket.once('open', onOpen);
            socket.once('error', onError);
            socket.on('close', onClose);

            // ----------------------------------------------------------------
            // Обработчик входящих сообщений
            // ----------------------------------------------------------------
            socket.on('message', (data: RawData) => {
                // RawData может быть Buffer / ArrayBuffer,
                // поэтому приводим к строке
                const text = data.toString();

                let parsed: any = null;

                // Пытаемся распарсить JSON
                try {
                    parsed = JSON.parse(text);
                } catch {
                    // Если сообщение не JSON — просто игнорируем
                    return;
                }

                // ----------------------------------------------------------------
                // Pong / Ping сообщения от Bybit
                // ----------------------------------------------------------------
                // Не шумим логами: pong/ping приходят часто и только засоряют вывод.
                if (parsed?.op === 'pong' || parsed?.op === 'ping') {
                    return;
                }

                // ----------------------------------------------------------------
                // Системные сообщения (ack на subscribe и т.п.)
                // ----------------------------------------------------------------
                if (typeof parsed?.success === 'boolean' || parsed?.ret_msg) {
                    const preview =
                        text.length > 300 ? `${text.slice(0, 300)}…` : text;

                    logger.info(`[BybitWS] Системное сообщение: ${preview}`);
                    return;
                }

                // ----------------------------------------------------------------
                // Рыночные данные: тикер
                // ----------------------------------------------------------------
                if (
                    typeof parsed?.topic === 'string' &&
                    parsed.topic.startsWith('tickers.')
                ) {
                    // Bybit V5 может прислать `data` как объект или массив.
                    // Нам нужен один "тикер-срез" (берём первый элемент, если это массив).
                    const raw = parsed?.data;
                    const ticker = Array.isArray(raw) ? raw[0] : raw;

                    // Если вдруг тикер пустой/неожиданный — просто игнорируем.
                    if (!ticker || typeof ticker !== 'object') return;

                    // В Bybit поля могут быть строками или числами. Мы приводим всё к строкам,
                    // сохраняя точность и предсказуемость типа для остальной системы.
                    const toStr = (v: unknown): string | undefined => {
                        if (v === undefined || v === null) return undefined;
                        // Важно: не отбрасываем 0
                        const s = String(v);
                        return s === '' ? undefined : s;
                    };

                    const tickerEvent = {
                        symbol: String(ticker.symbol ?? parsed.topic.split('.')[1]),
                        lastPrice: toStr(ticker.lastPrice ?? ticker.last_price),
                        markPrice: toStr(ticker.markPrice ?? ticker.mark_price),
                        indexPrice: toStr(ticker.indexPrice ?? ticker.index_price),
                        price24hPcnt: toStr(ticker.price24hPcnt ?? ticker.price_24h_pcnt),
                        highPrice24h: toStr(ticker.highPrice24h ?? ticker.high_price_24h),
                        lowPrice24h: toStr(ticker.lowPrice24h ?? ticker.low_price_24h),
                        volume24h: toStr(ticker.volume24h ?? ticker.volume_24h),
                        turnover24h: toStr(ticker.turnover24h ?? ticker.turnover_24h),
                        ts: typeof parsed?.ts === 'number' ? parsed.ts : undefined,
                    };

                    // Не эмитим пустые тикеры: Bybit иногда присылает сообщения по topic без нужных полей.
                    // Для нас в MVP критично иметь хотя бы lastPrice.
                    if (!tickerEvent.lastPrice) {
                        return;
                    }

                    // Публикуем событие в шину.
                    eventBus.emit('market:ticker', tickerEvent);

                    // Лёгкий лог без спама огромным JSON.
                    logger.info(
                        `[BybitWS] market:ticker emitted: ${tickerEvent.symbol} last=${tickerEvent.lastPrice ?? 'n/a'} 24h=${tickerEvent.price24hPcnt ?? 'n/a'}`
                    );
                    return;
                }
            });
        });
    }

    // ------------------------------------------------------------------------
    // disconnect
    // ------------------------------------------------------------------------
    // Ручное отключение WebSocket (graceful shutdown).
    // Используется при завершении приложения.
    // ВАЖНО: стараемся закрыться "мягко" (close), и только по таймауту — terminate().
    // ------------------------------------------------------------------------
    async disconnect(): Promise<void> {
        if (!this.socket) return;

        const socket = this.socket;

        logger.info('[BybitWS] Отключаемся вручную...');

        // Останавливаем heartbeat заранее, чтобы не слать ping во время закрытия
        this.stopHeartbeat();

        // Если сокет уже закрывается/закрыт — просто чистим состояние
        if (
            socket.readyState === WebSocket.CLOSING ||
            socket.readyState === WebSocket.CLOSED
        ) {
            this.cleanupSocket('manual disconnect (already closing/closed)');
            return;
        }

        // Ждём событие close, но не бесконечно
        const closeTimeoutMs = 2_000;

        await new Promise<void>((resolve) => {
            const timeout = setTimeout(() => {
                // Если сервер не закрыл соединение — добиваем terminate
                try {
                    socket.terminate();
                } catch {
                    // ignore
                }
                this.cleanupSocket('manual disconnect (timeout -> terminate)');
                resolve();
            }, closeTimeoutMs);

            // Если close всё-таки пришёл — закрылись корректно
            socket.once('close', () => {
                clearTimeout(timeout);
                this.cleanupSocket('manual disconnect (close event)');
                resolve();
            });

            try {
                socket.close();
            } catch {
                clearTimeout(timeout);
                this.cleanupSocket('manual disconnect (close threw)');
                resolve();
            }
        });
    }

    // ------------------------------------------------------------------------
    // subscribeTicker
    // ------------------------------------------------------------------------
    // Отправляет подписку на поток тикера для указанного символа.
    //
    // Пример:
    //   subscribeTicker('BTCUSDT')
    //   → подписка на `tickers.BTCUSDT`
    // ------------------------------------------------------------------------
    subscribeTicker(symbol: string): void {
        // Проверяем, что WebSocket существует и открыт
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.error('[BybitWS] Нельзя подписаться: WebSocket не открыт');
            return;
        }

        // Формируем сообщение подписки в формате Bybit V5
        const msg = {
            op: 'subscribe',
            args: [`tickers.${symbol}`],
        };

        // Отправляем сообщение
        this.sendJson(msg);

        // Логируем факт отправки подписки
        logger.info(`[BybitWS] Подписка отправлена: tickers.${symbol}`);
    }

    // ------------------------------------------------------------------------
    // sendJson
    // ------------------------------------------------------------------------
    // Универсальный метод отправки JSON-сообщений в WebSocket.
    // Используется всеми методами подписок.
    // ------------------------------------------------------------------------
    private sendJson(payload: unknown): void {
        // Проверяем, что сокет открыт
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
            logger.error('[BybitWS] Нельзя отправить сообщение: WebSocket не открыт');
            return;
        }

        // Отправляем JSON
        this.socket.send(JSON.stringify(payload));
    }

    // ------------------------------------------------------------------------
    // startHeartbeat
    // ------------------------------------------------------------------------
    // Запускает периодическую отправку ping,
    // чтобы соединение не разорвалось по таймауту.
    // ------------------------------------------------------------------------
    private startHeartbeat(): void {
        // Если таймер уже запущен — ничего не делаем
        if (this.heartbeatTimer) return;

        // Каждые 20 секунд отправляем ping
        this.heartbeatTimer = setInterval(() => {
            try {
                this.sendJson({ op: 'ping' });
            } catch (err) {
                // Ошибки отправки пинга могут означать проблемы с сокетом.
                // Не падаем, но фиксируем для диагностики.
                logger.error('[BybitWS] Ошибка отправки ping:', err);
            }
        }, 20_000);

        logger.info('[BybitWS] Heartbeat запущен (ping каждые 20с)');
    }

    // ------------------------------------------------------------------------
    // stopHeartbeat
    // ------------------------------------------------------------------------
    // Останавливает таймер heartbeat.
    // ------------------------------------------------------------------------
    private stopHeartbeat(): void {
        if (!this.heartbeatTimer) return;

        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;

        logger.info('[BybitWS] Heartbeat остановлен');
    }
}
