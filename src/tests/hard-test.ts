/*
 * HARD TEST MODE — lifecycle/reconnect/shutdown invariants
 * Запуск: npx ts-node src/tests/hard-test.ts
 * Цель: смоделировать жёсткие сценарии без реального биржевого подключения.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { BybitPublicWsClient } from '../exchange/bybit/wsClient';
import { Orchestrator } from '../control/orchestrator';
import { eventBus, type ControlCommand, type ControlState } from '../core/events/EventBus';
import { logger } from '../infra/logger';

// --------- helpers ---------
const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const waitFor = async (pred: () => boolean, timeoutMs: number, stepMs = 25) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (pred()) return true;
        // eslint-disable-next-line no-await-in-loop
        await wait(stepMs);
    }
    return false;
};
const assert = (cond: boolean, msg: string) => {
    if (!cond) throw new Error(`ASSERT FAIL: ${msg}`);
};

// Слушаем события, фиксируем последовательность
const controlStates: ControlState[] = [];
const controlCommands: ControlCommand[] = [];
const logLines: string[] = [];
const lastState = (): ControlState | undefined => controlStates[controlStates.length - 1];

logger.setLevel('debug');
logger.setDisplay(true);
logger.setSink((_entry, formatted) => {
    logLines.push(formatted);
    // Дублируем в консоль, чтобы видеть прогресс при запуске
    // eslint-disable-next-line no-console
    console.log(formatted);
});

// --------- mock WS server ---------
class MockWsServer {
    private wss: WebSocketServer;
    private sockets = new Set<WebSocket>();

    constructor(public readonly port: number) {
        this.wss = new WebSocketServer({ port });
        this.wss.on('connection', (socket) => {
            this.sockets.add(socket);
            socket.on('close', () => this.sockets.delete(socket));
            socket.on('message', (data) => {
                // Ответ на ping -> pong, остальное игнорируем
                try {
                    const parsed = JSON.parse(data.toString());
                    if (parsed?.op === 'ping') {
                        socket.send(JSON.stringify({ op: 'pong' }));
                    }
                } catch {
                    // ignore
                }
            });
        });
    }

    sendTicker(symbol = 'TESTUSD', price = '100'): void {
        const payload = JSON.stringify({ topic: `tickers.${symbol}`, data: { symbol, lastPrice: price } });
        for (const s of this.sockets) s.send(payload);
    }

    sendSystemAck(): void {
        const payload = JSON.stringify({ success: true, req_id: 'mock' });
        for (const s of this.sockets) s.send(payload);
    }

    forceClose(code = 1001): void {
        for (const s of this.sockets) s.close(code, 'forced');
    }

    stop(): void {
        this.wss.close();
    }
}

// --------- scenario runner ---------
async function run() {
    // Поднимаем mock WS
    const port = 18080;
    const server = new MockWsServer(port);

    // Поднимаем orchestrator + WS client
    const orchestrator = new Orchestrator();
    const wsClient = new BybitPublicWsClient(`ws://localhost:${port}`, {
        pingIntervalMs: 200,
        watchdogMs: 1_200,
    });

    let cleanupRuns = 0;
    orchestrator.registerCleanup(async () => {
        cleanupRuns += 1;
        await wsClient.disconnect();
    });

    eventBus.subscribe('control:state', (s) => controlStates.push({ ...s }));
    eventBus.subscribe('control:command', (c) => controlCommands.push(c));

    orchestrator.start();
    await wsClient.connect();

    // Boot test: STARTING until first ticker
    assert(controlStates[0]?.lifecycle === 'STARTING', 'Should start in STARTING');
    wsClient.subscribeTicker('TESTUSD');
    server.sendSystemAck();
    await wait(20);
    assert(lastState()?.lifecycle === 'STARTING', 'Should still be STARTING before ticker');

    server.sendTicker('TESTUSD', '101');
    await wait(50);
    assert(lastState()?.lifecycle === 'RUNNING', 'Should switch to RUNNING after first ticker');

    // Forced WS failure → reconnect, lifecycle stays RUNNING
    server.forceClose();
    // ждём пока reconnect поднимет новое соединение (до 1.5s)
    const reconnected = await waitFor(() => wsClient.getStatus().state === 'open', 1_500, 50);
    assert(reconnected, 'WebSocket should reconnect and reach open state');
    assert(lastState()?.lifecycle === 'RUNNING', 'Lifecycle must stay RUNNING on reconnect');

    // Heartbeat stall simulation: просто молчим дольше watchdog, клиент сам инициирует reconnect
    await wait(1_500);
    assert(lastState()?.lifecycle === 'RUNNING', 'Lifecycle must stay RUNNING on watchdog reconnect');

    // Command storm
    const publish = (cmd: ControlCommand) => eventBus.publish('control:command', cmd);
    publish({ type: 'pause', meta: { source: 'cli', ts: Date.now() } });
    publish({ type: 'pause', meta: { source: 'cli', ts: Date.now() } });
    publish({ type: 'resume', meta: { source: 'cli', ts: Date.now() } });
    publish({ type: 'resume', meta: { source: 'cli', ts: Date.now() } });
    publish({ type: 'shutdown', meta: { source: 'cli', ts: Date.now() }, reason: 'storm exit' });
    publish({ type: 'shutdown', meta: { source: 'cli', ts: Date.now() }, reason: 'storm exit dup' });
    publish({ type: 'shutdown', meta: { source: 'cli', ts: Date.now() }, reason: 'storm exit trip' });

    await wait(200);

    const stopCount = controlStates.filter((s) => s.lifecycle === 'STOPPING').length;
    const stoppedCount = controlStates.filter((s) => s.lifecycle === 'STOPPED').length;
    assert(stopCount === 1, 'STOPPING should happen once');
    assert(stoppedCount === 1, 'STOPPED should happen once');
    assert(cleanupRuns === 1, 'Cleanup must run once');

    // ------- дополнительные инварианты по логам -------
    const heartbeatStarts = logLines.filter((l) => l.includes('Heartbeat запущен')).length;
    const opens = logLines.filter((l) => l.includes('Соединение установлено')).length;
    assert(heartbeatStarts === opens, 'Heartbeat must start exactly once per open');

    // Между Cleanup и следующим open не должно быть старта heartbeat
    const cleanupIdx = logLines.findIndex((l) => l.includes('Cleanup: close'));
    const nextHeartbeatIdx = logLines.findIndex((l, idx) => idx > cleanupIdx && l.includes('Heartbeat запущен'));
    const nextOpenIdx = logLines.findIndex((l, idx) => idx > cleanupIdx && l.includes('Соединение установлено'));
    if (cleanupIdx >= 0 && nextHeartbeatIdx >= 0 && nextOpenIdx >= 0) {
        assert(nextHeartbeatIdx > nextOpenIdx, 'Heartbeat must not start before next open after cleanup');
    }

    const resubscribeLogs = logLines.filter((l) => l.includes('Re-subscribe: tickers.TESTUSD')).length;
    assert(resubscribeLogs >= 1, 'Re-subscribe should happen after reconnect');

    const startingCount = controlStates.filter((s) => s.lifecycle === 'STARTING').length;
    const runningCount = controlStates.filter((s) => s.lifecycle === 'RUNNING').length;
    assert(startingCount >= 1 && runningCount >= 1, 'Lifecycle should include STARTING then RUNNING');

    logger.info('[TEST] Hard test completed OK');

    server.stop();
    process.exit(0);
}

run().catch((err) => {
    logger.error(`[TEST] Hard test failed: ${String(err)}`);
    process.exitCode = 1;
    setTimeout(() => process.exit(1), 200);
});
