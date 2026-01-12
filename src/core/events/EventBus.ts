import EventEmitter from 'eventemitter3';
import { randomUUID } from 'node:crypto';

// ============================================================================
// Shared meta
// ----------------------------------------------------------------------------
// Базовые поля для трассировки событий в production:
// - source: кто инициатор (CLI/Telegram/System/Risk/etc.)
// - correlationId: связывает цепочку событий в один "flow" (потом пригодится для трассировки)
// - ts: unix ms (когда событие создано внутри бота)
// ============================================================================

// ---------------------------------------------------------------------------
// Общие типы
// ---------------------------------------------------------------------------

// Откуда пришло событие/команда (помогает понимать, кто инициировал действие)
export type EventSource =
    | 'cli'
    | 'telegram'
    | 'system'
    | 'market'
    | 'analytics'
    | 'strategy'
    | 'risk'
    | 'trading'
    | 'storage'
    | 'research';

export interface EventMeta {
    source: EventSource;
    ts: number; // unix ms
    correlationId?: string; // связывает цепочку событий
}

// ---------------------------------------------------------------------------
// Meta helpers (единый способ создавать meta по всему проекту)
// ---------------------------------------------------------------------------

/**
 * Date.now() в виде функции, чтобы легче тестировать и мокать.
 */
export const nowMs = (): number => Date.now();

/**
 * Генератор correlationId. В Node 18+ есть crypto.randomUUID().
 */
export const newCorrelationId = (): string => randomUUID();

/**
 * Создать meta для события/команды.
 *
 * Почему так:
 * - все события в системе должны иметь meta
 * - correlationId связывает цепочку событий (трассировка)
 * - source помогает понять, откуда пришло действие
 */
export function createMeta(
    source: EventSource,
    opts: {
        correlationId?: string;
        ts?: number;
    } = {}
): EventMeta {
    return {
        source,
        ts: opts.ts ?? nowMs(),
        correlationId: opts.correlationId,
    };
}

/**
 * Унаследовать correlationId из родительского события (если он был).
 * Удобно, когда одно событие порождает другое.
 */
export function inheritMeta(
    parent: EventMeta,
    source: EventSource,
    opts: {
        ts?: number;
    } = {}
): EventMeta {
    return {
        source,
        ts: opts.ts ?? nowMs(),
        correlationId: parent.correlationId ?? parent.ts.toString(),
    };
}

// ---------------------------------------------------------------------------
// Base event
// ---------------------------------------------------------------------------

export interface BaseEvent {
    meta: EventMeta;
}

// ============================================================================
// EventBus
// ----------------------------------------------------------------------------
// Централизованный брокер событий для всего торгового бота.
// Позволяет модулям обмениваться сообщениями без прямых зависимостей.
//
// ВАЖНО:
// - Сюда летят "нормализованные" события (наши собственные типы),
//   а не сырые ответы Bybit.
// - Это основа для масштабирования: MarketStore, Analytics, Strategy и т.д.
// ============================================================================

// ---------------------------------------------------------------------------
// Нормализованное событие "тикер" (market:ticker)
//
// Мы сознательно НЕ тащим сюда Bybit-специфичные структуры/названия.
// Внутри бота мы работаем со стабильным контрактом.
// ---------------------------------------------------------------------------
export interface TickerEvent extends BaseEvent {
    // Символ рынка (например BTCUSDT)
    symbol: string;

    // Цена последней сделки/обновления (строка, потому что Bybit присылает числа как строки)
    lastPrice?: string;

    // Mark price (цена маркировки, важна для деривативов)
    markPrice?: string;

    // Index price (индексная цена)
    indexPrice?: string;

    // Изменение за 24 часа (доля), например "-0.021485" ≈ -2.1485%
    price24hPcnt?: string;

    // Максимум/минимум за 24 часа
    highPrice24h?: string;
    lowPrice24h?: string;

    // Объём и оборот за 24 часа
    volume24h?: string;
    turnover24h?: string;

    // Биржевой timestamp сообщения (если доступен)
    exchangeTs?: number;
}

// ---------------------------------------------------------------------------
// Control Plane: команды и состояние бота
// ---------------------------------------------------------------------------

// Режимы работы бота (верхнеуровневая бизнес-логика)
export type BotMode = 'LIVE' | 'PAPER' | 'BACKTEST';

export type BotLifecycle = 'STARTING' | 'RUNNING' | 'PAUSED' | 'STOPPING' | 'STOPPED' | 'ERROR';

export type ControlCommand =
    | { type: 'pause'; meta: EventMeta; reason?: string }
    | { type: 'resume'; meta: EventMeta; reason?: string }
    | { type: 'status'; meta: EventMeta }
    | { type: 'shutdown'; meta: EventMeta; reason?: string }
    | { type: 'set_mode'; mode: BotMode; meta: EventMeta; reason?: string };

export interface ControlState extends BaseEvent {
    mode: BotMode;
    paused: boolean;
    lifecycle: BotLifecycle;
    startedAt: number;
    lastCommandAt: number;
    lastCommand?: ControlCommand['type'];
    lastCommandReason?: string;
    shuttingDown?: boolean;
}

// ---------------------------------------------------------------------------
// Strategy / Risk / Trading: намерения, решения и результаты
// ---------------------------------------------------------------------------

// Минимальная форма TradeIntent (пока без деталей). Дальше расширим.
export type TradeIntentAction = 'OPEN_LONG' | 'OPEN_SHORT' | 'CLOSE' | 'NONE';

export interface TradeIntent extends BaseEvent {
    id: string; // idempotency key (например uuid)
    symbol: string;
    action: TradeIntentAction;
    // Набор объяснений/метаданных (почему стратегия так решила)
    details?: Record<string, unknown>;
}

export interface ApprovedIntent extends BaseEvent {
    intent: TradeIntent;
    // рассчитанный объём/плечо/ограничения (позже).
    details?: Record<string, unknown>;
}

export interface RejectedIntent extends BaseEvent {
    intent: TradeIntent;
    reason: string;
    details?: Record<string, unknown>;
}

// Простые результаты исполнения (позже заменим на нормальные DTO биржи)
export interface OrderEvent extends BaseEvent {
    symbol: string;
    orderId?: string;
    status: 'NEW' | 'AMENDED' | 'CANCELED' | 'FILLED' | 'REJECTED' | 'UNKNOWN';
    side?: 'BUY' | 'SELL';
    price?: string;
    qty?: string;
    details?: Record<string, unknown>;
}

export interface PositionEvent extends BaseEvent {
    symbol: string;
    side: 'LONG' | 'SHORT' | 'FLAT';
    size: string; // строка для совместимости с биржей
    entryPrice?: string;
    unrealizedPnl?: string;
    details?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Market lifecycle (event-driven connect/disconnect/subscribe)
// ---------------------------------------------------------------------------

export interface MarketConnectRequest extends BaseEvent {
    url?: string;
    subscriptions?: string[]; // например: ['tickers.BTCUSDT']
}

export interface MarketDisconnectRequest extends BaseEvent {
    reason?: string;
}

export interface MarketSubscribeRequest extends BaseEvent {
    topics: string[]; // например: ['tickers.BTCUSDT']
}

export interface MarketConnected extends BaseEvent {
    url?: string;
    details?: Record<string, unknown>;
}

export interface MarketDisconnected extends BaseEvent {
    reason?: string;
    details?: Record<string, unknown>;
}

export interface MarketErrorEvent extends BaseEvent {
    phase: 'connect' | 'disconnect' | 'subscribe' | 'unknown';
    message: string;
    error?: unknown;
}

// ---------------------------------------------------------------------------
// Analytics: признаки (features) и контекст рынка
// ---------------------------------------------------------------------------

// Features = подготовленные «признаки» для Strategy/ML. Это не решения.
export interface FeaturesComputed extends BaseEvent {
    symbol: string;
    timeframe?: string; // например "1m", "5m" (когда появятся свечи)
    features: Record<string, number>; // EMA, RSI, ATR и т.д. в числах
}

// Context = агрегированное «состояние рынка» (режим/волатильность/тренд)
export interface MarketContextUpdated extends BaseEvent {
    symbol: string;
    context: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Storage/Recovery + Errors
// ---------------------------------------------------------------------------

export interface StateSnapshot extends BaseEvent {
    // snapshot может включать режим, позиции, последние свечи и т.д.
    data: Record<string, unknown>;
}

export interface BotErrorEvent extends BaseEvent {
    scope: 'system' | 'ws' | 'rest' | 'market' | 'analytics' | 'strategy' | 'risk' | 'trading';
    message: string;
    error?: unknown;
    details?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Research / Discovery plane
// ---------------------------------------------------------------------------

export type DriftSeverity = 'low' | 'medium' | 'high';

export interface MarketDriftDetected extends BaseEvent {
    symbol: string;
    window: {
        currentSize: number;
        baselineSize: number;
    };
    stats: Record<string, { meanDelta?: number; varDelta?: number }>;
    severity: DriftSeverity;
}

export interface FeatureVectorRecorded extends BaseEvent {
    symbol: string;
    timeframe?: string;
    featureVersion?: string;
    features: Record<string, number>;
}

export interface OutcomeRecorded extends BaseEvent {
    symbol: string;
    horizonMs: number;
    outcome: {
        pnl?: number;
        mae?: number;
        mfe?: number;
        hit?: boolean;
    };
    context?: Record<string, unknown>;
}

export interface NewClusterFound extends BaseEvent {
    clusterId: string;
    summary?: Record<string, unknown>;
}

export interface CandidateScenarioBuilt extends BaseEvent {
    scenarioId: string;
    source?: { clusterId?: string; ruleId?: string };
    entry: Record<string, unknown>;
    exit: Record<string, unknown>;
    riskDefaults?: Record<string, unknown>;
}

export interface BacktestCompleted extends BaseEvent {
    scenarioId: string;
    period: { from: number; to: number };
    metrics: { sharpe?: number; ddMax?: number; ev?: number; hitRate?: number; trades?: number };
    verdict: 'pass' | 'fail';
    notes?: string;
}

export interface ScenarioApproved extends BaseEvent {
    scenarioId: string;
    constraints?: Record<string, unknown>;
}

export interface ScenarioRejected extends BaseEvent {
    scenarioId: string;
    reason: string;
}

export interface ScenarioEnabled extends BaseEvent {
    scenarioId: string;
    reason?: string;
}

export interface ScenarioDisabled extends BaseEvent {
    scenarioId: string;
    reason?: string;
}

export interface RollbackRequested extends BaseEvent {
    reason?: string;
    targetScenarioId?: string;
}

// ---------------------------------------------------------------------------
// Карта всех событий бота.
//
// ВАЖНО: EventEmitter3 лучше всего типизируется через map "topic -> args tuple".
// Тогда:
// - emit(topic, ...args) типобезопасен
// - on(topic, (...args) => {}) типобезопасен
//
// Мы сознательно делаем ПООДНОМУ payload-объекту на событие.
// То есть каждый topic несёт 1 аргумент: (payload).
// ---------------------------------------------------------------------------
export type BotEventMap = {
    // Market Data
    'market:ticker': [payload: TickerEvent];

    // Control Plane
    'control:command': [payload: ControlCommand];
    'control:state': [payload: ControlState];

    // Analytics
    'analytics:features': [payload: FeaturesComputed];
    'analytics:context': [payload: MarketContextUpdated];

    // Strategy
    'strategy:intent': [payload: TradeIntent];

    // Risk
    'risk:approved_intent': [payload: ApprovedIntent];
    'risk:rejected_intent': [payload: RejectedIntent];

    // Trading / Execution
    'exec:order_event': [payload: OrderEvent];
    'portfolio:position_update': [payload: PositionEvent];

    // Market lifecycle (commands + signals)
    'market:connect': [payload: MarketConnectRequest];
    'market:disconnect': [payload: MarketDisconnectRequest];
    'market:subscribe': [payload: MarketSubscribeRequest];
    'market:connected': [payload: MarketConnected];
    'market:disconnected': [payload: MarketDisconnected];
    'market:error': [payload: MarketErrorEvent];

    // Storage / Recovery
    'state:snapshot': [payload: StateSnapshot];
    'state:recovery': [payload: StateSnapshot];

    // Errors
    'error:event': [payload: BotErrorEvent];

    // Research / Discovery
    'analytics:marketDriftDetected': [payload: MarketDriftDetected];
    'research:featureVectorRecorded': [payload: FeatureVectorRecorded];
    'research:outcomeRecorded': [payload: OutcomeRecorded];
    'research:newClusterFound': [payload: NewClusterFound];
    'research:candidateScenarioBuilt': [payload: CandidateScenarioBuilt];
    'research:backtestCompleted': [payload: BacktestCompleted];
    'research:scenarioApproved': [payload: ScenarioApproved];
    'research:scenarioRejected': [payload: ScenarioRejected];
    'strategy:scenarioEnabled': [payload: ScenarioEnabled];
    'strategy:scenarioDisabled': [payload: ScenarioDisabled];
    'control:rollbackModel': [payload: RollbackRequested];
};

export type BotEventName = keyof BotEventMap;

// Типизированный EventBus (EventEmitter3 поддерживает generic map)
export class EventBus extends EventEmitter<BotEventMap> {
    /**
     * Явный alias для emit, чтобы в коде читалось как pub/sub.
     *
     * Мы сознательно делаем 1 payload на событие (а не несколько аргументов),
     * чтобы типизация была стабильной и не ломалась на union tuple.
     */
    public publish<T extends BotEventName>(topic: T, payload: BotEventMap[T][0]): boolean {
        // Небольшой каст нужен из-за пересечения union tuple в типах eventemitter3
        return (super.emit as any)(topic, payload);
    }

    /**
     * Явный alias для on, чтобы в коде читалось как pub/sub.
     */
    public subscribe<T extends BotEventName>(topic: T, handler: (...args: BotEventMap[T]) => void): this {
        return super.on(topic, handler);
    }

    /**
     * Удобное отписывание.
     */
    public unsubscribe<T extends BotEventName>(topic: T, handler: (...args: BotEventMap[T]) => void): this {
        return super.off(topic, handler);
    }
}

export const eventBus = new EventBus();