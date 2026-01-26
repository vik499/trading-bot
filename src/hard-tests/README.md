# Hard Test (Lifecycle/Reconnect/Shutdown)

Запуск:
- Один прогон: `npm run test:hard`
- Три прогона подряд: `npm run test:hard:loop`

Что проверяет:
- STARTING -> RUNNING только после первого `market:ticker`.
- Принудительный close (1001 forced) приводит к reconnect без смены lifecycle.
- Watchdog по входящим данным инициирует reconnect, lifecycle остаётся RUNNING.
- Шторм команд pause/resume/shutdown даёт ровно один STOPPING, один STOPPED, один cleanup.
- Heartbeat запускается ровно один раз на каждое open, не стартует между cleanup и новым open, re-subscribe происходит после reconnect.

Тест использует mock WebSocket-сервер: отвечает pong на ping, отдаёт тикеры, форсирует close.
Время интервалов в тесте ускорено (ping 200ms, watchdog 1200ms).
