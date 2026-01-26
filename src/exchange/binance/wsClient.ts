import WebSocket from 'ws';
import { logger } from '../../infra/logger';
import { m } from '../../core/logMarkers';
import { createMeta, eventBus, nowMs, type EventBus } from '../../core/events/EventBus';
import { normalizeSymbol } from '../../core/market/symbols';
import type {
    KlineEvent,
    KlineInterval,
    LiquidationEvent,
    MarketType,
    VenueId,
    OrderbookL2DeltaEvent,
    OrderbookL2SnapshotEvent,
    OrderbookLevel,
    TickerEvent,
    TradeEvent,
} from '../../core/events/EventBus';
import { BinancePublicRestClient } from './restClient';
import {
    mapCandleRaw,
    mapIndexPriceRaw,
    mapLiquidationRaw,
    mapMarkPriceRaw,
    mapOrderbookDeltaRaw,
    mapOrderbookSnapshotRaw,
    mapTradeRaw,
} from '../normalizers/rawAdapters';

export interface BinanceWsClientOptions {
    streamId?: string;
    marketType?: MarketType;
    depthLimit?: number;
    reconnectMaxMs?: number;
    pingIntervalMs?: number;
    now?: () => number;
    restClient?: BinancePublicRestClient;
    supportsLiquidations?: boolean;
    supportsOrderbook?: boolean;
    eventBus?: EventBus;
}

type WsConnState = 'idle' | 'connecting' | 'open' | 'closing';

type DepthUpdate = {
    symbol: string;
    eventTs: number;
    firstUpdateId: number;
    lastUpdateId: number;
    bids: OrderbookLevel[];
    asks: OrderbookLevel[];
};

type DepthState = {
    snapshot?: { lastUpdateId: number; bids: OrderbookLevel[]; asks: OrderbookLevel[] };
    lastUpdateId?: number;
    buffered: DepthUpdate[];
    snapshotEmitted: boolean;
    snapshotInFlight: boolean;
};

const DEFAULT_STREAM_ID = 'binance.usdm.public';
const DEFAULT_WS_URL = 'wss://fstream.binance.com/ws';

export class BinancePublicWsClient {
    private socket: WebSocket | null = null;
    private state: WsConnState = 'idle';
    private connectPromise: Promise<void> | null = null;
    private isDisconnecting = false;
    private reconnectAttempts = 0;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private lastReconnectDelayMs?: number;
    private pingTimer: NodeJS.Timeout | null = null;
    private readonly subscriptions = new Set<string>();
    private readonly sentSubscriptions = new Set<string>();
    private readonly depthState = new Map<string, DepthState>();
    private readonly orderbookSymbols = new Set<string>();

    private readonly url: string;
    private readonly streamId: string;
    private readonly marketType?: MarketType;
    private readonly depthLimit: number;
    private readonly reconnectMaxMs: number;
    private readonly pingIntervalMs: number;
    private readonly supportsLiquidations: boolean;
    private readonly supportsOrderbook: boolean;
    private readonly restClient: BinancePublicRestClient;
    private readonly now: () => number;
    private readonly bus: EventBus;
    private readonly venue: VenueId = 'binance';

    constructor(url: string = DEFAULT_WS_URL, opts: BinanceWsClientOptions = {}) {
        this.url = url;
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.marketType = opts.marketType ?? 'futures';
        this.depthLimit = Math.max(50, opts.depthLimit ?? 500);
        this.reconnectMaxMs = Math.max(5_000, opts.reconnectMaxMs ?? 30_000);
        this.pingIntervalMs = Math.max(10_000, opts.pingIntervalMs ?? 20_000);
        this.supportsLiquidations = opts.supportsLiquidations ?? this.marketType === 'futures';
        this.supportsOrderbook = opts.supportsOrderbook ?? true;
        this.restClient = opts.restClient ?? new BinancePublicRestClient();
        this.now = opts.now ?? nowMs;
        this.bus = opts.eventBus ?? eventBus;
    }

    async connect(): Promise<void> {
        if (this.connectPromise) return this.connectPromise;
        if (this.socket && this.socket.readyState === WebSocket.OPEN) return;

        this.state = 'connecting';
        this.isDisconnecting = false;

        this.connectPromise = new Promise((resolve, reject) => {
            const socket = new WebSocket(this.url);
            this.socket = socket;

            const cleanup = (reason: string, closeCode?: number) => {
                if (this.socket) {
                    this.socket.removeAllListeners();
                }
                this.socket = null;
                this.state = 'idle';
                this.connectPromise = null;
                this.stopPing();
                this.sentSubscriptions.clear();
                if (!this.isDisconnecting) {
                    this.scheduleReconnect(reason, closeCode);
                }
            };

            socket.on('open', () => {
                this.state = 'open';
                this.reconnectAttempts = 0;
                this.sentSubscriptions.clear();
                this.startPing();
                this.flushSubscriptions();
                this.refreshOrderbookSnapshots();
                logger.info(m('ok', '[BinanceWS] connection open'));
                resolve();
            });

            socket.on('message', (raw) => {
                this.onMessage(raw.toString());
            });

            socket.on('close', (code: number, reason: Buffer) => {
                const reasonText = reason?.toString?.() ?? '';
                logger.warn(m('warn', `[BinanceWS] close code=${code}${reasonText ? ` reason=${reasonText}` : ''}`));
                cleanup('close', code);
            });

            socket.on('error', (err) => {
                logger.warn(m('warn', `[BinanceWS] error: ${(err as Error).message}`));
                if (this.state === 'connecting') {
                    cleanup('connect-error');
                    reject(err);
                }
            });
        });

        return this.connectPromise;
    }

    async disconnect(): Promise<void> {
        this.isDisconnecting = true;
        this.stopPing();
        if (!this.socket) return;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        const socket = this.socket;
        this.state = 'closing';
        await new Promise<void>((resolve) => {
            socket.once('close', () => resolve());
            try {
                socket.close();
            } catch {
                resolve();
            }
        });
    }

    subscribeTicker(symbol: string): void {
        const key = normalizeSymbol(symbol);
        if (!key) return;
        if (this.marketType !== 'spot') {
            this.subscribeStream(`${key.toLowerCase()}@markPrice@1s`);
        }
        this.subscribeStream(`${key.toLowerCase()}@ticker`);
    }

    subscribeTrades(symbol: string): void {
        const key = normalizeSymbol(symbol);
        if (!key) return;
        const suffix = this.marketType === 'spot' ? 'trade' : 'aggTrade';
        this.subscribeStream(`${key.toLowerCase()}@${suffix}`);
    }

    subscribeOrderbook(symbol: string, _depth?: number): void {
        if (!this.supportsOrderbook) return;
        const key = normalizeSymbol(symbol);
        if (!key) return;
        this.orderbookSymbols.add(key);
        this.subscribeStream(`${key.toLowerCase()}@depth@100ms`);
        this.ensureDepthSnapshot(key).catch((err) => {
            logger.warn(m('warn', `[BinanceWS] depth snapshot failed for ${key}: ${(err as Error).message}`));
        });
    }

    subscribeKlines(symbol: string, interval: KlineInterval): void {
        const key = normalizeSymbol(symbol);
        if (!key) return;
        const binanceInterval = toBinanceInterval(interval);
        if (!binanceInterval) return;
        this.subscribeStream(`${key.toLowerCase()}@kline_${binanceInterval}`);
    }

    subscribeLiquidations(symbol: string): void {
        if (!this.supportsLiquidations) return;
        const key = normalizeSymbol(symbol);
        if (!key) return;
        this.subscribeStream(`${key.toLowerCase()}@forceOrder`);
    }

    private subscribeStream(stream: string): void {
        if (this.subscriptions.has(stream)) return;
        this.subscriptions.add(stream);
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        if (this.sentSubscriptions.has(stream)) return;
        this.sentSubscriptions.add(stream);
        this.sendJson({ method: 'SUBSCRIBE', params: [stream], id: nextId() });
    }

    private flushSubscriptions(): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        if (this.subscriptions.size === 0) return;
        const params = Array.from(this.subscriptions.values()).filter((stream) => !this.sentSubscriptions.has(stream));
        if (params.length === 0) return;
        params.forEach((stream) => this.sentSubscriptions.add(stream));
        this.sendJson({ method: 'SUBSCRIBE', params, id: nextId() });
        logger.info(m('connect', `[BinanceWS] subscribed ${params.length} streams`));
    }

    private async ensureDepthSnapshot(symbol: string): Promise<void> {
        const state = this.depthState.get(symbol) ?? { buffered: [], snapshotEmitted: false, snapshotInFlight: false };
        if (state.snapshotInFlight) return;
        state.snapshotInFlight = true;
        this.depthState.set(symbol, state);
        try {
            const snapshot = await this.restClient.fetchDepthSnapshot({ symbol, limit: this.depthLimit });
            state.snapshot = snapshot;
            state.lastUpdateId = snapshot.lastUpdateId;

            const exchangeTs = this.now();
            const rawBids = snapshot.bids.map((lvl) => [String(lvl.price), String(lvl.size)] as [string, string]);
            const rawAsks = snapshot.asks.map((lvl) => [String(lvl.price), String(lvl.size)] as [string, string]);
            this.bus.publish(
                'market:orderbook_snapshot_raw',
                mapOrderbookSnapshotRaw(this.venue, symbol, rawBids, rawAsks, exchangeTs, exchangeTs, snapshot.lastUpdateId, this.marketType)
            );
        } finally {
            state.snapshotInFlight = false;
            this.depthState.set(symbol, state);
        }
    }

    private onMessage(raw: string): void {
        let parsed: Record<string, unknown>;
        try {
            parsed = JSON.parse(raw) as Record<string, unknown>;
        } catch {
            return;
        }

        if (parsed.result !== undefined || parsed.id !== undefined) {
            return;
        }

        const eventType = parsed.e;
        if (eventType === 'trade' || eventType === 'aggTrade') {
            this.handleTrade(parsed);
            return;
        }
        if (eventType === 'depthUpdate') {
            this.handleDepth(parsed);
            return;
        }
        if (eventType === 'kline') {
            this.handleKline(parsed);
            return;
        }
        if (eventType === 'markPriceUpdate') {
            this.handleMarkPrice(parsed);
            return;
        }
        if (eventType === '24hrTicker') {
            this.handleTicker(parsed);
            return;
        }
        if (eventType === 'forceOrder') {
            this.handleLiquidation(parsed);
            return;
        }
    }

    private handleTrade(payload: Record<string, unknown>): void {
        const symbol = String(payload.s ?? 'n/a');
        const price = toOptionalNumber(payload.p);
        const size = toOptionalNumber(payload.q);
        const tradeTs = toOptionalNumber(payload.T ?? payload.E);
        const isBuyerMaker = Boolean(payload.m);
        const side: TradeEvent['side'] = isBuyerMaker ? 'Sell' : 'Buy';
        if (price === undefined || size === undefined || tradeTs === undefined) return;

        const tradeId = payload.t ?? payload.a;
        const tradeEvent: TradeEvent = {
            symbol,
            streamId: this.streamId,
            tradeId: tradeId !== undefined ? String(tradeId) : undefined,
            side,
            price,
            size,
            tradeTs,
            exchangeTs: tradeTs,
            marketType: this.marketType,
            meta: createMeta('market', { ts: tradeTs }),
        } as TradeEvent;
        this.bus.publish('market:trade', tradeEvent);

        const rawEvt = mapTradeRaw(this.venue, symbol, payload, tradeTs, this.now(), this.marketType);
        if (rawEvt) this.bus.publish('market:trade_raw', rawEvt);
    }

    private handleDepth(payload: Record<string, unknown>): void {
        const symbol = String(payload.s ?? 'n/a');
        const firstUpdateId = toOptionalNumber(payload.U);
        const lastUpdateId = toOptionalNumber(payload.u);
        const eventTs = toOptionalNumber(payload.E);
        if (firstUpdateId === undefined || lastUpdateId === undefined || eventTs === undefined) return;

        const bids = parseDepthLevels(payload.b);
        const asks = parseDepthLevels(payload.a);
        const update: DepthUpdate = { symbol, eventTs, firstUpdateId, lastUpdateId, bids, asks };

        this.bus.publish(
            'market:orderbook_delta_raw',
            mapOrderbookDeltaRaw(
                this.venue,
                symbol,
                payload.b,
                payload.a,
                eventTs,
                this.now(),
                lastUpdateId,
                firstUpdateId,
                { start: firstUpdateId, end: lastUpdateId },
                this.marketType
            )
        );

        const state = this.depthState.get(symbol) ?? { buffered: [], snapshotEmitted: false, snapshotInFlight: false };
        state.buffered.push(update);
        this.depthState.set(symbol, state);

        if (!state.snapshot) return;
        this.flushDepth(symbol, state);
    }

    private flushDepth(symbol: string, state: DepthState): void {
        if (!state.snapshot) return;
        const lastSnapshotId = state.snapshot.lastUpdateId;
        const sorted = state.buffered
            .slice()
            .sort((a, b) => a.firstUpdateId - b.firstUpdateId || a.lastUpdateId - b.lastUpdateId || a.eventTs - b.eventTs);
        const usable = sorted.filter((evt) => evt.lastUpdateId > lastSnapshotId);
        state.buffered = usable;

        if (!state.snapshotEmitted) {
            const firstIndex = usable.findIndex(
                (evt) => evt.firstUpdateId <= lastSnapshotId + 1 && evt.lastUpdateId >= lastSnapshotId + 1
            );
            if (firstIndex === -1) {
                if (usable.length > 0 && usable[0].firstUpdateId > lastSnapshotId + 1) {
                    this.requestResync(symbol, 'gap', { lastSnapshotId });
                }
                this.depthState.set(symbol, state);
                return;
            }

            const first = usable[firstIndex];
            const snapshotEvent: OrderbookL2SnapshotEvent = {
                symbol,
                streamId: this.streamId,
                updateId: lastSnapshotId,
                exchangeTs: first.eventTs,
                marketType: this.marketType,
                bids: state.snapshot.bids,
                asks: state.snapshot.asks,
                meta: createMeta('market', { ts: first.eventTs }),
            } as OrderbookL2SnapshotEvent;
            this.bus.publish('market:orderbook_l2_snapshot', snapshotEvent);
            state.snapshotEmitted = true;
            state.lastUpdateId = lastSnapshotId;
            state.buffered = usable.slice(firstIndex);
        }

        for (const evt of state.buffered) {
            if (state.lastUpdateId !== undefined && evt.lastUpdateId <= state.lastUpdateId) continue;
            if (state.lastUpdateId !== undefined && evt.firstUpdateId > state.lastUpdateId + 1) {
                this.requestResync(symbol, 'gap', { lastUpdateId: state.lastUpdateId, nextUpdateId: evt.firstUpdateId });
                this.depthState.set(symbol, state);
                return;
            }
            state.lastUpdateId = evt.lastUpdateId;
            const delta: OrderbookL2DeltaEvent = {
                symbol,
                streamId: this.streamId,
                updateId: evt.lastUpdateId,
                exchangeTs: evt.eventTs,
                marketType: this.marketType,
                bids: evt.bids,
                asks: evt.asks,
                meta: createMeta('market', { ts: evt.eventTs }),
            } as OrderbookL2DeltaEvent;
            this.bus.publish('market:orderbook_l2_delta', delta);
        }
        state.buffered = [];
        this.depthState.set(symbol, state);
    }

    private handleKline(payload: Record<string, unknown>): void {
        const kline = payload.k as Record<string, unknown> | undefined;
        if (!kline) return;
        const isFinal = Boolean(kline.x);
        if (!isFinal) return;

        const symbol = String(payload.s ?? kline.s ?? 'n/a');
        const startTs = toOptionalNumber(kline.t);
        const endTs = toOptionalNumber(kline.T ?? kline.t);
        const open = toOptionalNumber(kline.o);
        const high = toOptionalNumber(kline.h);
        const low = toOptionalNumber(kline.l);
        const close = toOptionalNumber(kline.c);
        const volume = toOptionalNumber(kline.v);
        const interval = toKlineInterval(String(kline.i ?? ''));
        if (
            interval === undefined ||
            startTs === undefined ||
            endTs === undefined ||
            open === undefined ||
            high === undefined ||
            low === undefined ||
            close === undefined ||
            volume === undefined
        ) {
            return;
        }

        const event: KlineEvent = {
            symbol,
            interval,
            tf: intervalToTf(interval),
            startTs,
            endTs,
            open,
            high,
            low,
            close,
            volume,
            streamId: this.streamId,
            marketType: this.marketType,
            meta: createMeta('market', { ts: endTs }),
        } as KlineEvent;
        this.bus.publish('market:kline', event);

        const rawEvt = mapCandleRaw(
            this.venue,
            symbol,
            intervalToTf(interval),
            startTs,
            endTs,
            kline.o,
            kline.h,
            kline.l,
            kline.c,
            kline.v,
            true,
            endTs,
            this.now(),
            this.marketType
        );
        this.bus.publish('market:candle_raw', rawEvt);
    }

    private handleMarkPrice(payload: Record<string, unknown>): void {
        const symbol = String(payload.s ?? 'n/a');
        const markPrice = payload.p !== undefined ? String(payload.p) : undefined;
        const indexPrice = payload.i !== undefined ? String(payload.i) : undefined;
        const eventTs = toOptionalNumber(payload.E);
        if (!eventTs) return;

        const evt: TickerEvent = {
            symbol,
            streamId: this.streamId,
            marketType: this.marketType,
            markPrice,
            indexPrice,
            exchangeTs: eventTs,
            meta: createMeta('market', { ts: eventTs }),
        } as TickerEvent;
        this.bus.publish('market:ticker', evt);

        const recvTs = this.now();
        if (markPrice !== undefined) {
            this.bus.publish('market:mark_price_raw', mapMarkPriceRaw(this.venue, symbol, markPrice, eventTs, recvTs, this.marketType));
        }
        if (indexPrice !== undefined) {
            this.bus.publish('market:index_price_raw', mapIndexPriceRaw(this.venue, symbol, indexPrice, eventTs, recvTs, this.marketType));
        }
    }

    private handleTicker(payload: Record<string, unknown>): void {
        const symbol = String(payload.s ?? 'n/a');
        const eventTs = toOptionalNumber(payload.E);
        if (!eventTs) return;
        const evt: TickerEvent = {
            symbol,
            streamId: this.streamId,
            marketType: this.marketType,
            lastPrice: payload.c !== undefined ? String(payload.c) : undefined,
            price24hPcnt: payload.P !== undefined ? String(Number(payload.P) / 100) : undefined,
            highPrice24h: payload.h !== undefined ? String(payload.h) : undefined,
            lowPrice24h: payload.l !== undefined ? String(payload.l) : undefined,
            volume24h: payload.v !== undefined ? String(payload.v) : undefined,
            turnover24h: payload.q !== undefined ? String(payload.q) : undefined,
            exchangeTs: eventTs,
            meta: createMeta('market', { ts: eventTs }),
        } as TickerEvent;
        this.bus.publish('market:ticker', evt);
    }

    private handleLiquidation(payload: Record<string, unknown>): void {
        if (!this.supportsLiquidations) return;
        const order = payload.o as Record<string, unknown> | undefined;
        if (!order) return;
        const symbol = String(order.s ?? payload.s ?? 'n/a');
        const sideRaw = order.S ?? order.side;
        const side = sideRaw === 'BUY' ? 'Buy' : sideRaw === 'SELL' ? 'Sell' : undefined;
        const price = toOptionalNumber(order.p ?? order.ap);
        const size = toOptionalNumber(order.q ?? order.z);
        const eventTs = toOptionalNumber(payload.E ?? order.T);
        const evt: LiquidationEvent = {
            symbol,
            streamId: this.streamId,
            side,
            price,
            size,
            notionalUsd: price !== undefined && size !== undefined ? price * size : undefined,
            exchangeTs: eventTs,
            marketType: this.marketType,
            meta: createMeta('market', { ts: eventTs ?? this.now() }),
        } as LiquidationEvent;
        this.bus.publish('market:liquidation', evt);

        if (eventTs !== undefined) {
            const rawEvt = mapLiquidationRaw(
                this.venue,
                symbol,
                eventTs,
                this.now(),
                { side: side?.toLowerCase(), price, size, notionalUsd: evt.notionalUsd },
                this.marketType
            );
            this.bus.publish('market:liquidation_raw', rawEvt);
        }
    }

    private startPing(): void {
        if (this.pingTimer) return;
        this.pingTimer = setInterval(() => {
            if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
            try {
                this.socket.ping();
            } catch {
                // ignore
            }
        }, this.pingIntervalMs);
    }

    private stopPing(): void {
        if (this.pingTimer) {
            clearInterval(this.pingTimer);
            this.pingTimer = null;
        }
    }

    isAlive(): boolean {
        return Boolean(this.socket && this.socket.readyState === WebSocket.OPEN);
    }

    private requestResync(symbol: string, reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown', details?: Record<string, unknown>): void {
        this.resetDepthState(symbol);
        this.bus.publish('market:resync_requested', {
            venue: this.venue,
            symbol,
            channel: 'orderbook',
            reason,
            streamId: this.streamId,
            details,
            meta: createMeta('market', { ts: this.now() }),
        });
        void this.ensureDepthSnapshot(symbol);
    }

    private resetDepthState(symbol: string): void {
        const state = this.depthState.get(symbol) ?? { buffered: [], snapshotEmitted: false, snapshotInFlight: false };
        state.snapshot = undefined;
        state.lastUpdateId = undefined;
        state.buffered = [];
        state.snapshotEmitted = false;
        state.snapshotInFlight = false;
        this.depthState.set(symbol, state);
    }

    private refreshOrderbookSnapshots(): void {
        for (const symbol of this.orderbookSymbols) {
            this.resetDepthState(symbol);
            void this.ensureDepthSnapshot(symbol);
        }
    }

    private scheduleReconnect(reason: string, closeCode?: number): void {
        if (this.isDisconnecting) return;
        if (this.reconnectTimer) return;
        this.reconnectAttempts += 1;
        const base = Math.min(this.reconnectMaxMs, 1000 * Math.pow(2, this.reconnectAttempts - 1));
        const minCooldown = closeCode === 1008 ? 5_000 : 1_000;
        const baseWithMin = Math.max(base, minCooldown);
        const jitter = Math.floor(baseWithMin * 0.2 * stableJitterFactor(`binance:${reason}`, this.reconnectAttempts));
        const delay = baseWithMin + jitter;
        this.lastReconnectDelayMs = delay;
        logger.warn(m('warn', `[BinanceWS] reconnect in ${delay}ms (reason=${reason})`));
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            void this.connect();
        }, delay);
    }

    private sendJson(payload: unknown): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        try {
            this.socket.send(JSON.stringify(payload));
        } catch {
            // ignore
        }
    }
}

let idCounter = 1;
function nextId(): number {
    idCounter += 1;
    return idCounter;
}

function parseDepthLevels(raw: unknown): OrderbookLevel[] {
    if (!Array.isArray(raw)) return [];
    const levels: OrderbookLevel[] = [];
    for (const entry of raw) {
        if (!Array.isArray(entry)) continue;
        const price = toOptionalNumber(entry[0]);
        const size = toOptionalNumber(entry[1]);
        if (price === undefined || size === undefined) continue;
        if (!Number.isFinite(price) || !Number.isFinite(size)) continue;
        if (size <= 0) continue;
        levels.push({ price, size });
    }
    return levels;
}

function toOptionalNumber(value: unknown): number | undefined {
    if (value === undefined || value === null) return undefined;
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : undefined;
}

function toBinanceInterval(interval: KlineInterval): string | undefined {
    switch (interval) {
        case '1':
            return '1m';
        case '3':
            return '3m';
        case '5':
            return '5m';
        case '15':
            return '15m';
        case '30':
            return '30m';
        case '60':
            return '1h';
        case '120':
            return '2h';
        case '240':
            return '4h';
        case '360':
            return '6h';
        case '720':
            return '12h';
        case '1440':
            return '1d';
        default:
            return undefined;
    }
}

function toKlineInterval(interval: string): KlineInterval | undefined {
    const map: Record<string, KlineInterval> = {
        '1m': '1',
        '3m': '3',
        '5m': '5',
        '15m': '15',
        '30m': '30',
        '1h': '60',
        '2h': '120',
        '4h': '240',
        '6h': '360',
        '12h': '720',
        '1d': '1440',
    };
    return map[interval];
}

function intervalToTf(interval: KlineInterval): string {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return interval;
    if (minutes < 60) return `${minutes}m`;
    if (minutes % 1440 === 0) return `${minutes / 1440}d`;
    if (minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
}

function stableJitterFactor(seed: string, failures: number): number {
    const input = `${seed}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}
