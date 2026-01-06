import { logger } from '../infra/logger';
import { createMeta, eventBus } from '../core/events/EventBus';
import { m } from '../core/logMarkers';
import type { ControlCommand, ControlState, EventSource, TickerEvent } from '../core/events/EventBus';

type CleanupFn = () => Promise<void> | void;

/**
 * Orchestrator = “диспетчер системы”.
 * Единственный владелец lifecycle и режимов. Все переходы состояния идут через него.
 */
export class Orchestrator {
  private state: ControlState;
  private cleanupFns: CleanupFn[] = [];
  private commandUnsub?: () => void;
  private readinessUnsub?: () => void;
  private shuttingDown = false;
  private ready = false;

  constructor() {
    const now = Date.now();
    this.state = {
      meta: createMeta('system', { ts: now }),
      mode: 'LIVE',
      paused: false,
      lifecycle: 'STARTING',
      startedAt: now,
      lastCommandAt: now,
      lastCommand: undefined,
      lastCommandReason: undefined,
      shuttingDown: false,
    };
  }

  /**
   * Регистрируем “что надо аккуратно выключить”.
   * Например: wsClient.disconnect(), закрыть БД, остановить таймеры.
   */
  registerCleanup(fn: CleanupFn) {
    this.cleanupFns.push(fn);
  }

  start() {
    const handler = (cmd: ControlCommand) => {
      this.handleCommand(cmd).catch((err) => {
        logger.error(`[Orchestrator] command failed: ${(err as Error).message}`);
      });
    };

    eventBus.subscribe('control:command', handler);
    this.commandUnsub = () => eventBus.unsubscribe('control:command', handler);

    // Readiness: первый market:ticker переключает STARTING -> RUNNING (если не paused)
    const tickerHandler = (_t: TickerEvent) => {
      this.markReady('market');
    };
    eventBus.subscribe('market:ticker', tickerHandler);
    this.readinessUnsub = () => eventBus.unsubscribe('market:ticker', tickerHandler);

    this.publishState('system', 'status');
    logger.info(m('lifecycle', '[Orchestrator] started (STARTING)'));
  }

  stop() {
    this.commandUnsub?.();
    this.commandUnsub = undefined;
    this.readinessUnsub?.();
    this.readinessUnsub = undefined;
  }

  private publishState(source: EventSource, lastCommand?: ControlCommand['type'], reason?: string) {
    const now = Date.now();
    this.state = {
      ...this.state,
      meta: createMeta(source, { ts: now }),
      lastCommandAt: now,
      lastCommand: lastCommand ?? this.state.lastCommand,
      lastCommandReason: reason ?? this.state.lastCommandReason,
    };

    eventBus.publish('control:state', this.state);
  }

  private markReady(source: EventSource) {
    if (this.ready) return;
    if (this.shuttingDown) return;
    this.ready = true;
    this.state.lifecycle = this.state.paused ? 'PAUSED' : 'RUNNING';
    logger.info(m('ok', '[Orchestrator] readiness reached → RUNNING'));
    this.publishState(source, 'status');
  }

  private async handleCommand(cmd: ControlCommand) {
    if (this.shuttingDown && cmd.type !== 'status') {
      logger.debug('[Orchestrator] ignoring command during shutdown');
      return;
    }

    switch (cmd.type) {
      case 'status': {
        this.publishState(cmd.meta.source, 'status');
        return;
      }

      case 'pause': {
        if (!this.state.paused) {
          this.state.paused = true;
          this.state.lifecycle = 'PAUSED';
          logger.info(m('lifecycle', `[Orchestrator] pause requested${cmd.reason ? `: ${cmd.reason}` : ''}`));
        }
        this.publishState(cmd.meta.source, 'pause', cmd.reason);
        return;
      }

      case 'resume': {
        if (this.state.paused) {
          this.state.paused = false;
          this.state.lifecycle = this.ready ? 'RUNNING' : 'STARTING';
          logger.info(m('lifecycle', `[Orchestrator] resume to mode=${this.state.mode}${cmd.reason ? `: ${cmd.reason}` : ''}`));
        }
        this.publishState(cmd.meta.source, 'resume', cmd.reason);
        return;
      }

      case 'set_mode': {
        this.state.mode = cmd.mode;
        this.state.lifecycle = this.state.paused ? 'PAUSED' : this.ready ? 'RUNNING' : 'STARTING';
        logger.info(m('lifecycle', `[Orchestrator] mode -> ${cmd.mode}${this.state.paused ? ' (paused)' : ''}${cmd.reason ? `: ${cmd.reason}` : ''}`));
        this.publishState(cmd.meta.source, 'set_mode', cmd.reason);
        return;
      }

      case 'shutdown': {
        if (this.shuttingDown) {
          logger.debug('[Orchestrator] shutdown already in progress, ignoring duplicate command');
          return;
        }
        this.shuttingDown = true;
        logger.info(m('shutdown', `[Orchestrator] shutdown requested${cmd.reason ? `: ${cmd.reason}` : ''}`));
        this.state.lifecycle = 'STOPPING';
        this.state.shuttingDown = true;
        this.publishState(cmd.meta.source, 'shutdown', cmd.reason);

        await this.runCleanup();

        this.state.lifecycle = 'STOPPED';
        this.state.shuttingDown = true;
        this.publishState('system', 'shutdown', cmd.reason);
        return;
      }
    }
  }

  private async runCleanup() {
    this.stop();

    for (let i = this.cleanupFns.length - 1; i >= 0; i--) {
      try {
        await this.cleanupFns[i]?.();
      } catch (err) {
        logger.error(`[Orchestrator] cleanup error: ${(err as Error).message}`);
      }
    }
  }
}