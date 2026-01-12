import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { logger } from '../infra/logger';
import { BybitPublicWsClient } from '../exchange/bybit/wsClient';
import { Orchestrator } from '../control/orchestrator';
import {
    createMeta,
    eventBus,
    type ControlCommand,
    type ControlState,
    type EventSource,
    type TickerEvent,
} from '../core/events/EventBus';
import { MarketGateway } from '../exchange/marketGateway';
import { startCli } from '../cli/cliApp';
import { m } from '../core/logMarkers';

const APP_NAME = 'vavanbot';
const APP_TAG = `[${APP_NAME}]`;

// Синхронный лог: печатается даже если логгер буферизует.
const logSync = (msg: string) => {
    try {
        process.stderr.write(msg + '\n');
    } catch {
        // ignore
    }
};

type BotMode = 'LIVE' | 'PAPER' | 'BACKTEST';

type BuildInfo = {
    appVersion: string;
    gitVersion: string;
    nodeVersion: string;
    pid: number;
};

function getAppVersion(): string {
    try {
        // live-bot.ts лежит в src/apps → ../../package.json = корень проекта
        const pkgPath = path.resolve(__dirname, '../../package.json');
        const raw = fs.readFileSync(pkgPath, 'utf8');
        const pkg = JSON.parse(raw);
        if (pkg?.version) return String(pkg.version);
    } catch {
        // ignore
    }

    // Иногда npm прокидывает версию через env
    if (process.env.npm_package_version) return String(process.env.npm_package_version);

    return 'unknown';
}

function getGitDescribe(): string {
    try {
        return execSync('git describe --tags --dirty --always', {
            stdio: ['ignore', 'pipe', 'ignore'],
        })
            .toString()
            .trim();
    } catch {
        // например, если запуск без .git (docker/prod)
        return 'n/a';
    }
}

function getBuildInfo(): BuildInfo {
    return {
        appVersion: getAppVersion(),
        gitVersion: getGitDescribe(),
        nodeVersion: process.version,
        pid: process.pid,
    };
}

async function flushLogger(): Promise<void> {
    try {
        const anyLogger = logger as any;
        if (typeof anyLogger.flush === 'function') {
            await Promise.resolve(anyLogger.flush());
        } else if (typeof anyLogger.close === 'function') {
            await Promise.resolve(anyLogger.close());
        } else if (typeof anyLogger.end === 'function') {
            await Promise.resolve(anyLogger.end());
        }
    } catch {
        // ignore
    }
}

function emitControlCommand(cmd: { type: 'pause' | 'resume' | 'shutdown' | 'status'; source: EventSource; reason?: string }) {
    // Пока Control Plane не выделен в отдельный модуль, публикуем команды в EventBus.
    const controlCommand: ControlCommand = {
        type: cmd.type,
        reason: cmd.reason,
        meta: createMeta(cmd.source),
    } as ControlCommand;

    eventBus.publish('control:command', controlCommand);
}

class LiveBotApp {
    private mode: BotMode = 'LIVE';
    private paused = false;

    private wsClient?: BybitPublicWsClient;
    private marketGateway?: MarketGateway;
    private healthInterval?: NodeJS.Timeout;
    private orchestrator?: Orchestrator;

    private readonly enableCli = process.env.BOT_CLI !== '0';

    // handlers, чтобы на shutdown корректно отписаться
    private onTickerHandler?: (t: TickerEvent) => void;
    private onControlStateHandler?: (state: ControlState) => void;

    private controlState?: ControlState;
    private stopRequested = false;
    private stopped = false;
    private exitCode = 0;
    private stopTimeout?: NodeJS.Timeout;

    public async start(): Promise<void> {
        logger.info(m('lifecycle', `${APP_TAG} Запущен торговый бот (live mode)...`));

        const build = getBuildInfo();
        logger.info(
            m('ok', `${APP_TAG} Build info: appVersion=${build.appVersion} git=${build.gitVersion} node=${build.nodeVersion} pid=${build.pid}`)
        );

        // 1) Поднимаем Market Data Plane (пока только public WS)
        this.wsClient = new BybitPublicWsClient('wss://stream.bybit.com/v5/public/linear');
        this.marketGateway = new MarketGateway(this.wsClient);
        this.marketGateway.start();

        // 1.5) Поднимаем Control Plane (Orchestrator), регистрируем cleanup
        this.orchestrator = new Orchestrator();
        this.orchestrator.registerCleanup(async () => {
            if (this.marketGateway) await this.marketGateway.shutdown('orchestrator cleanup');
        });
        this.orchestrator.start();

        // 2) Подписки на EventBus (пока временно: логирование и управление режимом)
        this.wireEventBusSubscriptions();

        // Запрашиваем актуальное состояние, чтобы синхронизироваться после подписки
        emitControlCommand({ type: 'status', source: 'system' });

        // 3) Подключение и подписка теперь делаются через orchestrator->market events (см. orchestrator.start())

        // 5) Health-пульс. Позже это станет частью Control Plane: health/status.
        this.healthInterval = setInterval(() => {
            if (this.stopRequested) return;
            logger.info(m('lifecycle', `${APP_TAG} Status: mode=${this.mode} paused=${this.paused} lifecycle=${this.controlState?.lifecycle ?? 'n/a'} (bot alive)`));
            // TODO(ControlPlane): publish control:state from Orchestrator
        }, 30_000);

        // 6) Консольное управление (CLI): команды идут в EventBus как control:command
        if (this.enableCli) {
            startCli();
        } else {
            logger.info(`${APP_TAG} CLI disabled (BOT_CLI=0)`);
        }
    }

    private wireEventBusSubscriptions(): void {
        // market:ticker → выводим с троттлингом, чтобы не спамить консоль
        this.onTickerHandler = (t: TickerEvent) => {
            const symbol = t?.symbol ?? 'n/a';
            const lastPrice = t?.lastPrice ?? 'n/a';
            const p24h = t?.price24hPcnt ?? 'n/a';

            logger.infoThrottled(`ticker:${symbol}`, `${APP_TAG} Ticker: ${symbol} last=${lastPrice} 24h=${p24h}`, 3000);
        };
        eventBus.subscribe('market:ticker', this.onTickerHandler);

        this.onControlStateHandler = (state: ControlState) => {
            this.controlState = state;
            this.mode = state.mode;
            this.paused = state.paused;

            if (state.lifecycle === 'STOPPED') {
                logger.info(m('shutdown', `${APP_TAG} STOPPED получено от Orchestrator — завершаем процесс`));
                void this.completeStop();
            }
        };
        eventBus.subscribe('control:state', this.onControlStateHandler);
    }

    public requestShutdown(reason: string, exitCode = 0): void {
        if (this.stopRequested) return;
        this.stopRequested = true;
        this.exitCode = exitCode;

        logSync(`${APP_TAG} shutdown requested: ${reason}`);
        emitControlCommand({ type: 'shutdown', source: 'system', reason });

        // Фолбэк: если STOPPED не придёт, завершимся сами.
        this.stopTimeout = setTimeout(() => {
            logger.warn(`${APP_TAG} Shutdown timeout, forcing exit`);
            void this.completeStop(true);
        }, 5_000);
        this.stopTimeout.unref?.();
    }

    private async completeStop(force = false): Promise<void> {
        if (this.stopped) return;
        this.stopped = true;

        try {
            if (this.healthInterval) {
                clearInterval(this.healthInterval);
                this.healthInterval = undefined;
            }
        } catch {
            // ignore
        }

        try {
            if (this.onTickerHandler) {
                eventBus.unsubscribe('market:ticker', this.onTickerHandler);
                this.onTickerHandler = undefined;
            }
            if (this.onControlStateHandler) {
                eventBus.unsubscribe('control:state', this.onControlStateHandler);
                this.onControlStateHandler = undefined;
            }
        } catch {
            // ignore
        }

        try {
            this.orchestrator?.stop();
        } catch {
            // ignore
        }

        if (force && this.wsClient) {
            try {
                await this.wsClient.disconnect();
            } catch {
                // ignore
            }
        }

        if (this.stopTimeout) {
            clearTimeout(this.stopTimeout);
            this.stopTimeout = undefined;
        }

        await flushLogger();

        process.exitCode = this.exitCode;
        logSync(`${APP_TAG} shutdown complete, exitCode=${process.exitCode ?? 0}`);
        process.exit(process.exitCode ?? 0);
    }
}

async function main(): Promise<void> {
    const app = new LiveBotApp();

    const onFatal = async (label: string, err: unknown) => {
        logger.error(`${APP_TAG} ${label}:`, err);
        app.requestShutdown(label, 1);
    };

    process.on('unhandledRejection', (reason) => void onFatal('unhandledRejection', reason));
    process.on('uncaughtException', (err) => void onFatal('uncaughtException', err));

    // SIGINT/SIGTERM: публикуем команду, а shutdown делаем через app.
    process.once('SIGINT', () => {
        logSync(`${APP_TAG} SIGINT received`);
        app.requestShutdown('SIGINT', 0);
    });
    process.once('SIGTERM', () => {
        logSync(`${APP_TAG} SIGTERM received`);
        app.requestShutdown('SIGTERM', 0);
    });

    await app.start();
}

main().catch(async (error) => {
    try {
        process.stderr.write(`${APP_TAG} Fatal error in trading bot: ${String(error)}\n`);
    } catch {
        // ignore
    }
    logger.error(`${APP_TAG} Fatal error in trading bot:`, error);

    await flushLogger();

    process.exitCode = 1;
    setTimeout(() => process.exit(1), 250);
});
