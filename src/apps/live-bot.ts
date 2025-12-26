import { logger } from '../infra/logger';
import { BybitPublicWsClient } from '../exchange/bybit/wsClient';
import { eventBus } from '../core/events/EventBus';

// Точка входа для live-режима торгового бота.
// Запускает основной процесс работы: инициализацию сервисов,
// установление соединений с биржей и постоянный цикл выполнения.

async function main(): Promise<void> {
    logger.info('[vavanbot] Запущен торговый бот (live mode)...');

    // 1. Создаём экземпляр клиента для публичного WebSocket Bybit.
    const bybitWsClient = new BybitPublicWsClient(
        'wss://stream.bybit.com/v5/public/linear'
    );

    // 2. Пытаемся подключиться к WebSocket и ЖДЁМ, пока подключение установится.
    await bybitWsClient.connect();

    // 3. Если мы здесь — значит connect() отработал успешно.
    logger.info('[vavanbot] Подключение к Bybit WebSocket успешно, продолжаем работу бота');

    // 4. Пока у нас нет MarketStore, просто слушаем события и выводим в лог,
    // чтобы видеть, что нормализация и EventBus работают правильно.
    eventBus.on('market:ticker', (t) => {
        logger.info(
            `[vavanbot] TickerEvent: ${t.symbol} last=${t.lastPrice ?? 'n/a'} 24h=${t.price24hPcnt ?? 'n/a'}`
        );
    });

    // 5. Подписываемся на тикер BTCUSDT, чтобы начать получать реальные данные рынка.
    bybitWsClient.subscribeTicker('BTCUSDT');

    // Флаг, чтобы shutdown не выполнялся дважды.
    let isShuttingDown = false;

    // Чтобы гарантировать корректный выход даже при неожиданных ошибках.
    const onFatal = (label: string, err: unknown) => {
        logger.error(`[vavanbot] ${label}:`, err);
        void shutdown(label);
    };

    process.on('unhandledRejection', (reason) => onFatal('unhandledRejection', reason));
    process.on('uncaughtException', (err) => onFatal('uncaughtException', err));

    // Простая "пульсация" (health log) раз в 30 секунд — вместо спама каждые 5 секунд.
    const heartbeatInterval = setInterval(() => {
        logger.info('[vavanbot] Торговый бот работает...');
    }, 30_000);

    // Корректное завершение работы: закрываем WS и чистим интервалы.
    const shutdown = async (signal: string) => {
        if (isShuttingDown) return;
        isShuttingDown = true;

        logger.info(`[vavanbot] Получен сигнал ${signal}. Останавливаемся...`);

        clearInterval(heartbeatInterval);

        // На будущее: здесь будет логика pause/exit, отмена ордеров и т.д.
        // Сейчас минимум — корректно закрыть соединение.
        try {
            await Promise.resolve(bybitWsClient.disconnect());
        } catch (err) {
            logger.error('[vavanbot] Ошибка при отключении WS:', err);
        }

        process.exit(0);
    };

    process.once('SIGINT', () => void shutdown('SIGINT'));
    process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((error) => {
    logger.error('[vavanbot] Fatal error in trading bot:', error);
    process.exit(1);
});
