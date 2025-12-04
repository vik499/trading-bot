// Точка входа для live-режима торгового бота.
// Запускает основной процесс работы: инициализацию сервисов,
// установление соединений с биржей и постоянный цикл выполнения.

async function main(): Promise<void> {
    console.log('[vavanbot] Запущен торговый бот (live mode)...');

    setInterval(() => {
        console.log('[vavanbot] Торговый бот работает...');
    }, 5000);
}

main().catch((error) => {
    console.log ('[vavanbot] Fatal error in trading bot:', error);
    process.exit(1);
});