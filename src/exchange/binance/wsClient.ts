import WebSocket from 'ws';
import { logger } from '../../infra/logger';
import { m } from '../../core/logMarkers';
import { asSeq, asTsMs, createMeta, eventBus, nowMs, type EventBus } from '../../core/events/EventBus';
import { normalizeSymbol } from '../../core/market/symbols';
import type {
    KlineEvent,
    KlineInterval,
    LiquidationEvent,
    KnownMarketType,
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

const toErrorMessage = (err: unknown): string => {
    if (!err) return 'unknown error';
    if (err instanceof Error) return err.message;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
};

export interface BinanceWsClientOptions {
    streamId?: string;
    marketType?: KnownMarketType;
    depthLimit?: number;
    reconnectMaxMs?: number;
    pingIntervalMs?: number;
    now?: () => number;
    restClient?: BinancePublicRestClient;
    supportsLiquidations?: boolean;
    supportsOrderbook?: boolean;
    eventBus?: EventBus;
    instanceId?: string;
    instanceIdFactory?: () => string;
}

type WsConnState = 'idle' | 'connecting' | 'open' | 'closing';

type DepthUpdate = {
    symbol: string;
    eventTs: number;
    firstUpdateId: number;
    lastUpdateId: number;
    prevUpdateId?: number;
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

let binanceClientInstanceSeq = 0;
const nextBinanceClientInstanceId = (): string => {
    binanceClientInstanceSeq += 1;
    return binanceClientInstanceSeq.toString(36);
};

type BinanceClientRegistryKey = string;
const binanceClientRegistry = new Map<BinanceClientRegistryKey, BinancePublicWsClient>();

export const buildBinanceClientRegistryKey = (params: {
    venue: VenueId;
    streamId: string;
    symbol?: string;
    marketType?: KnownMarketType;
}): string => `${params.venue}:${params.streamId}:${params.symbol ?? '*'}:${params.marketType ?? 'unknown'}`;

export const resetBinancePublicWsClientRegistry = (): void => {
    binanceClientRegistry.clear();
};

export const getBinancePublicWsClient = (opts: BinanceWsClientOptions & { url?: string; symbol?: string }): BinancePublicWsClient => {
    const url = opts.url ?? DEFAULT_WS_URL;
    const streamId = opts.streamId ?? DEFAULT_STREAM_ID;
    const key = buildBinanceClientRegistryKey({ venue: 'binance', streamId, symbol: opts.symbol, marketType: opts.marketType });
    const cached = binanceClientRegistry.get(key);
    if (cached) return cached;
    const client = new BinancePublicWsClient(url, opts);
    logger.info(
        m(
            'connect',
            `[BinanceWS#${client.clientInstanceId}] creating BinanceWS client id=${client.clientInstanceId} venue=binance streamId=${streamId} symbol=${opts.symbol ?? 'n/a'} marketType=${opts.marketType ?? 'unknown'}`
        )
    );
    binanceClientRegistry.set(key, client);
    return client;
};

class BinanceSubscriptionManager {
    private readonly desired = new Map<string, string>();
    private readonly active = new Set<string>();
    private readonly pending = new Set<string>();

    requestSubscribe(stream: string): boolean {
        const key = buildBinanceSubKey(stream);
        if (this.desired.has(key)) return false;
        this.desired.set(key, stream);
        return true;
    }

    buildDiff(): Array<{ key: string; stream: string }> {
        const pendingKeys = new Set<string>([...this.active, ...this.pending]);
        const diffKeys = Array.from(this.desired.keys()).filter((key) => !pendingKeys.has(key));
        diffKeys.sort();
        return diffKeys.map((key) => ({ key, stream: this.desired.get(key)! }));
    }

    markPending(keys: string[]): void {
        keys.forEach((key) => this.pending.add(key));
    }

    markActive(key: string): void {
        this.pending.delete(key);
        this.active.add(key);
    }

    onOpen(): void {
        this.pending.clear();
        this.active.clear();
    }

    onClose(): void {
        this.pending.clear();
        this.active.clear();
    }

    desiredCount(): number {
        return this.desired.size;
    }

    activeCount(): number {
        return this.active.size;
    }

    pendingCount(): number {
        return this.pending.size;
    }

    /** Snapshot of current subscription state (for tests and diagnostics) */
    snapshot(): { desired: string[]; active: string[]; pending: string[] } {
        return {
            desired: Array.from(this.desired.values()),
            active: Array.from(this.active.values()),
            pending: Array.from(this.pending.values()),
        };
    }
}

export class BinancePublicWsClient {
    public readonly clientInstanceId: string;
    private socket: WebSocket | null = null;
    private state: WsConnState = 'idle';
    private connectPromise: Promise<void> | null = null;
    private isDisconnecting = false;
    private reconnectAttempts = 0;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private lastReconnectDelayMs?: number;
    private pingTimer: NodeJS.Timeout | null = null;
    private readonly subscriptions = new BinanceSubscriptionManager();
    private reconcileInFlight = false;
    private reconcileQueued = false;
    private readonly depthState = new Map<string, DepthState>();
    private readonly orderbookSymbols = new Set<string>();

    private readonly url: string;
    public readonly streamId: string;
    public readonly marketType?: KnownMarketType;
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
        this.clientInstanceId = opts.instanceId ?? (opts.instanceIdFactory ? opts.instanceIdFactory() : nextBinanceClientInstanceId());
        logger.info(m('connect', `[BinanceWS#${this.clientInstanceId}] created streamId=${this.streamId} marketType=${this.marketType ?? 'unknown'}`));
    }

    async connect(): Promise<void> {
        if (this.connectPromise) return this.connectPromise;
        if (this.socket && this.socket.readyState === WebSocket.OPEN) return;

        this.state = 'connecting';
        this.isDisconnecting = false;
        logger.info(m('connect', `[BinanceWS#${this.clientInstanceId}] connect start`));

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
                this.subscriptions.onClose();
                if (!this.isDisconnecting) {
                    this.scheduleReconnect(reason, closeCode);
                }
            };

            socket.on('open', () => {
                this.state = 'open';
                this.reconnectAttempts = 0;
                this.subscriptions.onOpen();
                this.startPing();
                this.flushSubscriptions();
                this.refreshOrderbookSnapshots();
                logger.info(m('ok', `[BinanceWS#${this.clientInstanceId}] connection open`));
                resolve();
            });

            socket.on('message', (raw) => {
                this.onMessage(raw.toString());
            });

            socket.on('close', (code: number, reason: Buffer) => {
                const reasonText = reason?.toString?.() ?? '';
                logger.warn(m('warn', `[BinanceWS#${this.clientInstanceId}] close code=${code}${reasonText ? ` reason=${reasonText}` : ''}`));
                cleanup('close', code);
            });

            socket.on('error', (err) => {
                logger.warn(m('warn', `[BinanceWS#${this.clientInstanceId}] error: ${(err as Error).message}`));
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
        this.subscriptions.requestSubscribe(stream);
        this.flushSubscriptions();
    }

    private flushSubscriptions(): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        if (this.reconcileInFlight) {
            this.reconcileQueued = true;
            return;
        }
        this.reconcileInFlight = true;
        try {
            const diff = this.subscriptions.buildDiff();
            if (diff.length === 0) return;
            const params = diff.map((entry) => entry.stream);
            const keys = diff.map((entry) => entry.key);
            this.sendJson({ method: 'SUBSCRIBE', params, id: nextId() });
            this.subscriptions.markPending(keys);
            keys.forEach((key) => this.subscriptions.markActive(key));
            logger.info(
                m(
                    'connect',
                    `[BinanceWS#${this.clientInstanceId}] subscribe diff=${diff.length} totalDesired=${this.subscriptions.desiredCount()} active=${this.subscriptions.activeCount()} pending=${this.subscriptions.pendingCount()}`
                )
            );
        } finally {
            this.reconcileInFlight = false;
            if (this.reconcileQueued) {
                this.reconcileQueued = false;
                this.flushSubscriptions();
            }
        }
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
            meta: createMeta('market', {
                tsEvent: asTsMs(tradeTs),
                tsExchange: asTsMs(tradeTs),
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
        } as TradeEvent;
        this.bus.publish('market:trade', tradeEvent);

        const rawEvt = mapTradeRaw(this.venue, symbol, payload, tradeTs, this.now(), this.marketType);
        if (rawEvt) this.bus.publish('market:trade_raw', rawEvt);
    }

    private handleDepth(payload: Record<string, unknown>): void {
        const symbol = String(payload.s ?? 'n/a');
        const firstUpdateId = toOptionalNumber(payload.U);
        const lastUpdateId = toOptionalNumber(payload.u);
        const prevUpdateId = toOptionalNumber(payload.pu);
        const eventTs = toOptionalNumber(payload.E);
        if (firstUpdateId === undefined || lastUpdateId === undefined || eventTs === undefined) return;

        const bids = parseDepthLevels(payload.b);
        const asks = parseDepthLevels(payload.a);
        const update: DepthUpdate = { symbol, eventTs, firstUpdateId, lastUpdateId, prevUpdateId, bids, asks };

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
                prevUpdateId ?? firstUpdateId,
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
        const isFutures = this.marketType === 'futures';
        const sorted = state.buffered
            .slice()
            .sort((a, b) => a.firstUpdateId - b.firstUpdateId || a.lastUpdateId - b.lastUpdateId || a.eventTs - b.eventTs);
        const usable = sorted.filter((evt) => (isFutures ? evt.lastUpdateId >= lastSnapshotId : evt.lastUpdateId > lastSnapshotId));
        state.buffered = usable;

        if (!state.snapshotEmitted) {
            const firstIndex = usable.findIndex((evt) =>
                isFutures
                    ? evt.firstUpdateId <= lastSnapshotId && evt.lastUpdateId >= lastSnapshotId
                    : evt.firstUpdateId <= lastSnapshotId + 1 && evt.lastUpdateId >= lastSnapshotId + 1
            );
            if (firstIndex === -1) {
                const nextExpectedId = isFutures ? lastSnapshotId : lastSnapshotId + 1;
                if (usable.length > 0 && usable[0].firstUpdateId > nextExpectedId) {
                    this.requestResync(symbol, 'gap', { lastSnapshotId, nextUpdateId: usable[0].firstUpdateId });
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
                meta: createMeta('market', {
                    tsEvent: asTsMs(first.eventTs),
                    tsExchange: asTsMs(first.eventTs),
                    tsIngest: asTsMs(this.now()),
                    sequence: asSeq(lastSnapshotId),
                    streamId: this.streamId,
                }),
            } as OrderbookL2SnapshotEvent;
            this.bus.publish('market:orderbook_l2_snapshot', snapshotEvent);
            state.snapshotEmitted = true;
            state.lastUpdateId = lastSnapshotId;
            state.buffered = usable.slice(firstIndex);
        }

        for (const evt of state.buffered) {
            if (state.lastUpdateId !== undefined) {
                const isStale = isFutures ? evt.lastUpdateId < state.lastUpdateId : evt.lastUpdateId <= state.lastUpdateId;
                if (isStale) continue;
                if (isFutures) {
                    if (
                        evt.prevUpdateId !== undefined &&
                        state.lastUpdateId !== lastSnapshotId &&
                        evt.prevUpdateId !== state.lastUpdateId
                    ) {
                        this.requestResync(symbol, 'out_of_order', {
                            lastUpdateId: state.lastUpdateId,
                            prevUpdateId: evt.prevUpdateId,
                            updateId: evt.lastUpdateId,
                        });
                        this.depthState.set(symbol, state);
                        return;
                    }
                } else if (evt.firstUpdateId > state.lastUpdateId + 1) {
                    this.requestResync(symbol, 'gap', { lastUpdateId: state.lastUpdateId, nextUpdateId: evt.firstUpdateId });
                    this.depthState.set(symbol, state);
                    return;
                }
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
                meta: createMeta('market', {
                    tsEvent: asTsMs(evt.eventTs),
                    tsExchange: asTsMs(evt.eventTs),
                    tsIngest: asTsMs(this.now()),
                    sequence: asSeq(evt.lastUpdateId),
                    streamId: this.streamId,
                }),
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

        const marketType = this.marketType;
        if (!isKnownMarketType(marketType)) {
            logger.warn(
                m('warn', `[BinanceWS#${this.clientInstanceId}] kline skipped for ${symbol}: marketType=${marketType ?? 'unknown'}`)
            );
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
            marketType,
            meta: createMeta('market', {
                tsEvent: asTsMs(endTs),
                tsExchange: asTsMs(endTs),
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
        };
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
            marketType
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
            meta: createMeta('market', {
                tsEvent: asTsMs(eventTs),
                tsExchange: asTsMs(eventTs),
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
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
            meta: createMeta('market', {
                tsEvent: asTsMs(eventTs),
                tsExchange: asTsMs(eventTs),
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
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
            meta: createMeta('market', {
                tsEvent: asTsMs(eventTs ?? this.now()),
                tsExchange: eventTs !== undefined ? asTsMs(eventTs) : undefined,
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
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
            meta: createMeta('market', { tsEvent: asTsMs(this.now()), tsIngest: asTsMs(this.now()) }),
        });
        this.ensureDepthSnapshot(symbol).catch((err) => {
            logger.warn(m('warn', `[BinanceWS] depth snapshot failed for ${symbol}: ${(err as Error).message}`));
        });
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
            this.ensureDepthSnapshot(symbol).catch((err) => {
                logger.warn(m('warn', `[BinanceWS] depth snapshot failed for ${symbol}: ${(err as Error).message}`));
            });
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
        logger.warn(m('warn', `[BinanceWS#${this.clientInstanceId}] reconnect in ${delay}ms (reason=${reason})`));
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect().catch((err) => {
                const msg = toErrorMessage(err);
                logger.warn(m('warn', `[BinanceWS#${this.clientInstanceId}] reconnect failed: ${msg}`));
            });
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

function buildBinanceSubKey(stream: string): string {
    return stream.trim().toLowerCase();
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

function isKnownMarketType(value: unknown): value is KnownMarketType {
    return value === 'spot' || value === 'futures';
}

function stableJitterFactor(seed: string, failures: number): number {
    const input = `${seed}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}
