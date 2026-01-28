import { logger } from '../infra/logger';
import { asTsMs, createMeta, eventBus, newCorrelationId } from '../core/events/EventBus';
import { m } from '../core/logMarkers';
import type {
  ControlCommand,
  ControlState,
  EventSource,
  KlineInterval,
  KlineBootstrapRequested,
  MarketConnectRequest,
  MarketSubscribeRequest,
  TickerEvent,
} from '../core/events/EventBus';

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
      meta: createMeta('system', { tsEvent: asTsMs(now) }),
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
    const bootCorrelationId = newCorrelationId();

    const symbols = readSymbols();
    const enableTrades = readFlag('BOT_TRADES_ENABLED', true);
    const enableOrderbook = readFlag('BOT_ORDERBOOK_ENABLED', true);
    const enableOi = readFlag('BOT_OI_ENABLED', true);
    const enableFunding = readFlag('BOT_FUNDING_ENABLED', true);
    const enableLiquidations = readFlag('BOT_LIQUIDATIONS_ENABLED', false);
    const enableKlines = readFlag('BOT_KLINES_ENABLED', true);
    const targetMarketType = readTargetMarketType();
    const enableSpot = targetMarketType ? targetMarketType === 'spot' : readFlag('BOT_SPOT_ENABLED', true);
    const orderbookDepthRaw = process.env.BOT_ORDERBOOK_DEPTH;
    const orderbookDepth = orderbookDepthRaw ? Number(orderbookDepthRaw) : undefined;

    const tfs = readKlineTfs();
    const buildTopics = (marketType: 'spot' | 'futures') => {
      const topics = new Set<string>();
      for (const symbol of symbols) {
        topics.add(`tickers.${symbol}`);
        if (enableTrades) topics.add(`trades.${symbol}`);
        if (enableOrderbook) {
          topics.add(orderbookDepth && Number.isFinite(orderbookDepth) ? `orderbook.${orderbookDepth}.${symbol}` : `orderbook.${symbol}`);
        }
        if (marketType !== 'spot') {
          if (enableOi) topics.add(`oi.${symbol}`);
          if (enableFunding) topics.add(`funding.${symbol}`);
          if (enableLiquidations) topics.add(`liquidations.${symbol}`);
        }
      }

      if (enableKlines) {
        for (const symbol of symbols) {
          for (const tf of tfs) {
            const interval = tfToInterval(tf);
            if (!interval) continue;
            topics.add(`kline.${interval}.${symbol}`);
          }
        }
      }

      return Array.from(topics);
    };

    const targets: Array<{ venue: MarketSubscribeRequest['venue']; marketType: MarketSubscribeRequest['marketType'] }> = [];
    if (!targetMarketType || targetMarketType === 'futures') {
      targets.push(
        { venue: 'bybit', marketType: 'futures' },
        { venue: 'binance', marketType: 'futures' },
        { venue: 'okx', marketType: 'futures' }
      );
    }
    if (!targetMarketType || targetMarketType === 'spot') {
      if (enableSpot) {
        targets.push(
          { venue: 'binance', marketType: 'spot' },
          { venue: 'okx', marketType: 'spot' }
        );
      }
    }

    for (const target of targets) {
      const connectPayload: MarketConnectRequest = {
        meta: createMeta('system', { correlationId: bootCorrelationId }),
        venue: target.venue,
        marketType: target.marketType,
      };
      eventBus.publish('market:connect', connectPayload);

      const marketTypeForTopics: 'spot' | 'futures' = target.marketType === 'spot' ? 'spot' : 'futures';
      const subscribePayload: MarketSubscribeRequest = {
        meta: createMeta('system', { correlationId: bootCorrelationId }),
        topics: buildTopics(marketTypeForTopics),
        venue: target.venue,
        marketType: target.marketType,
      };
      eventBus.publish('market:subscribe', subscribePayload);
    }

    const klineLimit = readKlineLimit();
    for (const symbol of symbols) {
      for (const tf of tfs) {
        const interval = tfToInterval(tf);
        if (!interval) continue;
        const bootstrapPayload: KlineBootstrapRequested = {
          meta: createMeta('system', { correlationId: bootCorrelationId }),
          symbol,
          interval,
          tf,
          limit: klineLimit,
        };
        eventBus.publish('market:kline_bootstrap_requested', bootstrapPayload);
      }
    }

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
      meta: createMeta(source, { tsEvent: asTsMs(now) }),
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

const DEFAULT_TFS = ['1m', '5m', '15m', '1h', '4h', '1d'];
const ALLOWED_INTERVALS: Set<KlineInterval> = new Set([
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

function readKlineTfs(): string[] {
  const raw = process.env.BOT_KLINE_TF ?? process.env.BOT_KLINE_INTERVALS;
  if (!raw) return DEFAULT_TFS;
  const list = raw
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  return list.length ? list : DEFAULT_TFS;
}

function readSymbols(): string[] {
  const raw = process.env.BOT_SYMBOLS ?? process.env.BOT_SYMBOL ?? 'BTCUSDT';
  const list = raw
    .split(',')
    .map((entry) => entry.trim().toUpperCase())
    .filter(Boolean);
  return list.length ? list : ['BTCUSDT'];
}

function readFlag(name: string, fallback = true): boolean {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
  if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
  return fallback;
}

function readTargetMarketType(): 'spot' | 'futures' | undefined {
  const raw = process.env.BOT_TARGET_MARKET_TYPE?.trim().toLowerCase();
  if (raw === 'spot' || raw === 'futures') return raw;
  return undefined;
}

function readKlineLimit(): number {
  const raw = process.env.BOT_KLINE_LIMIT;
  const parsed = raw ? Number.parseInt(raw, 10) : NaN;
  if (!Number.isFinite(parsed) || parsed <= 0) return 200;
  return parsed;
}

function tfToInterval(tf: string): KlineInterval | undefined {
  const match = tf.match(/^(\d+)([mhd])$/i);
  if (!match) return undefined;
  const value = Number.parseInt(match[1], 10);
  if (!Number.isFinite(value) || value <= 0) return undefined;
  const unit = match[2].toLowerCase();
  const minutes = unit === 'm' ? value : unit === 'h' ? value * 60 : value * 1440;
  const interval = String(minutes) as KlineInterval;
  if (!ALLOWED_INTERVALS.has(interval)) return undefined;
  return interval;
}
