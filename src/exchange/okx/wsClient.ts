import WebSocket from 'ws';
import { logger } from '../../infra/logger';
import { m } from '../../core/logMarkers';
import { asSeq, asTsMs, createMeta, eventBus, nowMs, type EventBus } from '../../core/events/EventBus';
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
    WsEventRaw,
} from '../../core/events/EventBus';
import {
    mapCandleRaw,
    mapIndexPriceRaw,
    mapLiquidationRaw,
    mapMarkPriceRaw,
    mapOrderbookDeltaRaw,
    mapOrderbookSnapshotRaw,
    mapTradeRaw,
} from '../normalizers/rawAdapters';

export interface OkxWsClientOptions {
    streamId?: string;
    marketType?: MarketType;
    pingIntervalMs?: number;
    reconnectMaxMs?: number;
    reconnectBaseMs?: number;
    backoffResetMs?: number;
    resyncCooldownMs?: number;
    now?: () => number;
    supportsLiquidations?: boolean;
    eventBus?: EventBus;
}

type WsConnState = 'idle' | 'connecting' | 'open' | 'closing';

type OkxInstType = 'SPOT' | 'SWAP' | 'FUTURES' | 'OPTION';
type OkxArg = { channel: string; instId: string; instType?: OkxInstType };

const DEFAULT_WS_URL = 'wss://ws.okx.com:8443/ws/v5/public';
const DEFAULT_STREAM_ID = 'okx.public.swap';
const DEFAULT_RECONNECT_BASE_MS = 500;
const DEFAULT_RECONNECT_MAX_MS = 8_000;
const DEFAULT_BACKOFF_RESET_MS = 20_000;

export class OkxPublicWsClient {
    private socket: WebSocket | null = null;
    private state: WsConnState = 'idle';
    private connectPromise: Promise<void> | null = null;
    private isDisconnecting = false;
    private reconnectAttempts = 0;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private pingTimer: NodeJS.Timeout | null = null;
    private stableTimer: NodeJS.Timeout | null = null;
    private lastReconnectDelayMs?: number;
    private lastProtocolError: { event: string; code?: string; msg?: string } | null = null;
    private lastError?: string;
    private readonly subscriptions = new Set<string>();
    private readonly sentSubscriptions = new Set<string>();
    private readonly orderbookSeq = new Map<string, number>();
    private readonly orderbookSnapshotReady = new Set<string>();
    private readonly orderbookLastAppliedAt = new Map<string, number>();
    private readonly orderbookPendingResync = new Set<string>();
    private readonly orderbookGapCount = new Map<string, number>();
    private readonly orderbookGapWindowStart = new Map<string, number>();
    private readonly resyncInFlightUntil = new Map<string, number>();
    private readonly lastResyncAt = new Map<string, number>();

    private readonly url: string;
    private readonly streamId: string;
    private readonly marketType?: MarketType;
    private readonly pingIntervalMs: number;
    private readonly resyncCooldownMs: number;
    private readonly resyncMinGapCount: number;
    private readonly resyncPendingMaxMs: number;
    private readonly supportsLiquidations: boolean;
    private readonly enableKlines: boolean;
    private readonly now: () => number;
    private readonly bus: EventBus;
    private readonly reconnectBaseMs: number;
    private readonly reconnectMaxMs: number;
    private readonly backoffResetMs: number;
    private readonly venue: VenueId = 'okx';

    constructor(url: string = DEFAULT_WS_URL, opts: OkxWsClientOptions = {}) {
        this.url = url;
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.marketType = opts.marketType;
        this.pingIntervalMs = Math.max(10_000, opts.pingIntervalMs ?? 20_000);
        this.resyncCooldownMs = Math.max(1_000, opts.resyncCooldownMs ?? 5_000);
        this.resyncMinGapCount = Math.max(1, Number(process.env.OKX_RESYNC_MIN_GAP_COUNT ?? 1));
        this.resyncPendingMaxMs = Math.max(1_000, Number(process.env.OKX_RESYNC_PENDING_MAX_MS ?? 5_000));
        this.supportsLiquidations = opts.supportsLiquidations ?? true;
        this.enableKlines = readFlag('OKX_ENABLE_KLINES', true);
        this.now = opts.now ?? nowMs;
        this.bus = opts.eventBus ?? eventBus;
        this.reconnectBaseMs = Math.max(100, opts.reconnectBaseMs ?? DEFAULT_RECONNECT_BASE_MS);
        this.reconnectMaxMs = Math.max(this.reconnectBaseMs, opts.reconnectMaxMs ?? DEFAULT_RECONNECT_MAX_MS);
        this.backoffResetMs = Math.max(1_000, opts.backoffResetMs ?? DEFAULT_BACKOFF_RESET_MS);
    }

    async connect(): Promise<void> {
        if (this.connectPromise) return this.connectPromise;
        if (this.socket && this.socket.readyState === WebSocket.OPEN) return;

        this.state = 'connecting';
        this.isDisconnecting = false;

        this.connectPromise = new Promise((resolve, reject) => {
            const socket = new WebSocket(this.url);
            this.socket = socket;

            const cleanup = (reason: string) => {
                if (this.socket) this.socket.removeAllListeners();
                this.socket = null;
                this.state = 'idle';
                this.connectPromise = null;
                this.stopPing();
                this.sentSubscriptions.clear();
                if (!this.isDisconnecting) {
                    this.scheduleReconnect(reason);
                }
            };

            socket.on('open', () => {
                this.state = 'open';
                this.reconnectAttempts = 0;
                this.lastProtocolError = null;
                this.lastError = undefined;
                this.orderbookPendingResync.clear();
                this.orderbookGapCount.clear();
                this.orderbookGapWindowStart.clear();
                this.sentSubscriptions.clear();
                this.scheduleStableReset();
                this.startPing();
                this.flushSubscriptions();
                logger.info(m('ok', '[OKXWS] connection open'));
                resolve();
            });

            socket.on('message', (raw) => {
                this.onMessage(raw.toString());
            });

            socket.on('close', (code: number, reason: Buffer) => {
                const reasonText = reason?.toString?.() ?? '';
                const lastError = this.lastProtocolError
                    ? ` lastError=${this.lastProtocolError.event}` +
                      `${this.lastProtocolError.code ? `:${this.lastProtocolError.code}` : ''}` +
                      `${this.lastProtocolError.msg ? `:${this.lastProtocolError.msg}` : ''}`
                    : '';
                const lastErrorText = this.lastError ? ` lastErrorText=${this.lastError}` : '';
                logger.warn(
                    m('warn', `[OKXWS] close code=${code}${reasonText ? ` reason=${reasonText}` : ''}${lastError}${lastErrorText}`)
                );
                this.clearStableReset();
                cleanup('close');
            });

            socket.on('error', (err) => {
                logger.warn(m('warn', `[OKXWS] error: ${(err as Error).message}`));
                this.clearStableReset();
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
        this.clearStableReset();
        this.orderbookPendingResync.clear();
        this.orderbookGapCount.clear();
        this.orderbookGapWindowStart.clear();
        this.sentSubscriptions.clear();
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
        const instId = toOkxInstId(symbol, this.marketType);
        if (!instId) return;
        this.subscribe({ channel: 'tickers', instId });
    }

    subscribeTrades(symbol: string): void {
        const instId = toOkxInstId(symbol, this.marketType);
        if (!instId) return;
        this.subscribe({ channel: 'trades', instId });
    }

    subscribeOrderbook(symbol: string, _depth?: number): void {
        const instId = toOkxInstId(symbol, this.marketType);
        if (!instId) return;
        this.subscribe({ channel: 'books', instId });
    }

    subscribeKlines(symbol: string, interval: KlineInterval): void {
        if (!this.enableKlines) return;
        if (this.url.includes('/ws/v5/public')) return;
        const instId = toOkxInstId(symbol, this.marketType);
        const channel = toOkxCandleChannel(interval);
        if (!instId || !channel) return;
        this.subscribe({ channel, instId });
    }

    subscribeLiquidations(symbol: string): void {
        if (!this.supportsLiquidations) return;
        const instId = toOkxInstId(symbol, this.marketType);
        if (!instId) return;
        this.subscribe({ channel: 'liquidation-orders', instId });
    }

    private normalizeSubArg(arg: OkxArg): OkxArg {
        if (arg.channel.startsWith('candle')) {
            return {
                channel: arg.channel,
                instId: arg.instId,
            };
        }
        return {
            channel: arg.channel,
            instId: arg.instId,
            instType: arg.instType ?? toOkxInstType(this.marketType),
        };
    }

    private subscribe(arg: OkxArg): void {
        const normalized = this.normalizeSubArg(arg);
        const key = buildSubKey(normalized);
        if (this.subscriptions.has(key)) {
            if (this.sentSubscriptions.has(key)) return;
        } else {
            this.subscriptions.add(key);
        }
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        if (this.sentSubscriptions.has(key)) return;
        this.sendJson({ op: 'subscribe', args: [normalized] });
        this.sentSubscriptions.add(key);
    }

    private flushSubscriptions(): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        if (this.subscriptions.size === 0) return;
        const pending = Array.from(this.subscriptions).filter((entry) => !this.sentSubscriptions.has(entry));
        if (pending.length === 0) return;
        const args: OkxArg[] = pending.map((entry) => JSON.parse(entry) as OkxArg);
        this.sendJson({ op: 'subscribe', args });
        pending.forEach((entry) => this.sentSubscriptions.add(entry));
        logger.info(m('connect', `[OKXWS] subscribed ${args.length} channels`));
    }

    private onMessage(raw: string): void {
        if (raw === 'pong') return;
        let parsed: Record<string, unknown>;
        try {
            parsed = JSON.parse(raw) as Record<string, unknown>;
        } catch {
            return;
        }
        if (parsed.event !== undefined) {
            const event = String(parsed.event);
            const code = parsed.code !== undefined ? String(parsed.code) : undefined;
            const msg = parsed.msg !== undefined ? String(parsed.msg) : undefined;
            const arg = parsed.arg as Record<string, unknown> | undefined;

            if (event === 'error') {
                const argPreview = arg ? ` arg=${JSON.stringify(arg)}` : '';
                logger.warn(
                    m('warn', `[OKXWS] event=${event}${code ? ` code=${code}` : ''}${msg ? ` msg=${msg}` : ''}${argPreview}`)
                );
                if (code === '60018' && arg?.channel && arg?.instId) {
                    const channel = String(arg.channel);
                    const instId = String(arg.instId);
                    const instType = arg.instType ? String(arg.instType) : toOkxInstType(this.marketType);
                    const subKey = buildSubKey({ channel, instId, instType: instType as OkxInstType });
                    this.subscriptions.delete(subKey);
                    this.sentSubscriptions.delete(subKey);
                    this.lastError = `error:${code}:${msg ?? ''}`;
                }
                this.lastProtocolError = { event, code, msg };
            } else if (event === 'subscribe' || event === 'unsubscribe') {
                logger.debug(m('ws', `[OKXWS] event=${event}${code ? ` code=${code}` : ''}`));
            } else {
                logger.info(
                    m('ws', `[OKXWS] event=${event}${code ? ` code=${code}` : ''}${msg ? ` msg=${msg}` : ''}`)
                );
            }

            const evt: WsEventRaw = {
                venue: this.venue,
                streamId: this.streamId,
                event,
                code,
                msg,
                payload: parsed,
                meta: createMeta('market', { tsEvent: asTsMs(this.now()), tsIngest: asTsMs(this.now()) }),
            };
            this.bus.publish('market:ws_event_raw', evt);
            return;
        }
        const arg = parsed.arg as Record<string, unknown> | undefined;
        const channel = arg?.channel ? String(arg.channel) : undefined;
        const instId = arg?.instId ? String(arg.instId) : undefined;
        const data = parsed.data;
        if (!channel || !Array.isArray(data) || !instId) return;

        if (channel === 'trades') {
            this.handleTrades(instId, data);
            return;
        }
        if (channel.startsWith('candle')) {
            this.handleKlines(instId, channel, data);
            return;
        }
        if (channel.startsWith('books')) {
            this.handleOrderbook(instId, parsed.action, data);
            return;
        }
        if (channel === 'tickers') {
            this.handleTicker(instId, data);
            return;
        }
        if (channel === 'liquidation-orders') {
            this.handleLiquidations(instId, data);
            return;
        }
    }

    private handleTrades(instId: string, rows: unknown[]): void {
        for (const row of rows) {
            if (!row || typeof row !== 'object') continue;
            const obj = row as Record<string, unknown>;
            const symbol = normalizeOkxSymbol(instId);
            const sideRaw = obj.side ? String(obj.side).toLowerCase() : undefined;
            const side = sideRaw === 'buy' ? 'Buy' : sideRaw === 'sell' ? 'Sell' : undefined;
            const price = toOptionalNumber(obj.px ?? obj.price);
            const size = toOptionalNumber(obj.sz ?? obj.size);
            const tradeTs = toOptionalNumber(obj.ts);
            if (!side || price === undefined || size === undefined || tradeTs === undefined) continue;

            const evt: TradeEvent = {
                symbol,
                streamId: this.streamId,
                tradeId: obj.tradeId !== undefined ? String(obj.tradeId) : undefined,
                side,
                price,
                size,
                tradeTs,
                exchangeTs: tradeTs,
                marketType: this.marketType ?? detectMarketType(instId),
                meta: createMeta('market', {
                    tsEvent: asTsMs(tradeTs),
                    tsExchange: asTsMs(tradeTs),
                    tsIngest: asTsMs(this.now()),
                    streamId: this.streamId,
                }),
            } as TradeEvent;
            this.bus.publish('market:trade', evt);

            const rawEvt = mapTradeRaw(this.venue, instId, obj, tradeTs, this.now(), this.marketType);
            if (rawEvt) this.bus.publish('market:trade_raw', rawEvt);
        }
    }

    private handleKlines(instId: string, channel: string, rows: unknown[]): void {
        const interval = toKlineInterval(channel);
        if (!interval) return;
        for (const row of rows) {
            if (!Array.isArray(row) || row.length < 6) continue;
            const startTs = toOptionalNumber(row[0]);
            const open = toOptionalNumber(row[1]);
            const high = toOptionalNumber(row[2]);
            const low = toOptionalNumber(row[3]);
            const close = toOptionalNumber(row[4]);
            const volume = toOptionalNumber(row[5]);
            const confirmRaw = row[8] ?? row[9];
            const confirmed = confirmRaw === undefined ? true : confirmRaw === '1' || confirmRaw === 1 || confirmRaw === true;
            if (!confirmed) continue;
            if (
                startTs === undefined ||
                open === undefined ||
                high === undefined ||
                low === undefined ||
                close === undefined ||
                volume === undefined
            ) {
                continue;
            }

            const endTs = startTs + intervalToMs(interval);
            const evt: KlineEvent = {
                symbol: normalizeOkxSymbol(instId),
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
                marketType: this.marketType ?? detectMarketType(instId),
                meta: createMeta('market', {
                    tsEvent: asTsMs(endTs),
                    tsExchange: asTsMs(endTs),
                    tsIngest: asTsMs(this.now()),
                    streamId: this.streamId,
                }),
            } as KlineEvent;
            this.bus.publish('market:kline', evt);

            const rawEvt = mapCandleRaw(
                this.venue,
                instId,
                intervalToTf(interval),
                startTs,
                endTs,
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                true,
                endTs,
                this.now(),
                this.marketType
            );
            this.bus.publish('market:candle_raw', rawEvt);
        }
    }

    private handleOrderbook(instId: string, action: unknown, rows: unknown[]): void {
        const payload = rows[0];
        if (!payload || typeof payload !== 'object') return;
        const obj = payload as Record<string, unknown>;
        const exchangeTs = toOptionalNumber(obj.ts);
        if (!exchangeTs) return;

        const symbol = normalizeOkxSymbol(instId);

        const seqId = toOptionalNumber(obj.seqId ?? obj.seq);
        const prevSeqId = toOptionalNumber(obj.prevSeqId);

        const bids = parseLevels(obj.bids);
        const asks = parseLevels(obj.asks);
        const updateId = seqId ?? Number.parseInt(String(exchangeTs), 10);
        const isSnapshot = String(action ?? '').toLowerCase() === 'snapshot';
        const lastSeq = this.orderbookSeq.get(symbol);
        if (isSnapshot) {
            this.orderbookSnapshotReady.add(symbol);
            this.orderbookGapCount.delete(symbol);
            this.orderbookGapWindowStart.delete(symbol);
            if (seqId !== undefined) this.orderbookSeq.set(symbol, seqId);
        } else {
            if (!this.orderbookSnapshotReady.has(symbol)) {
                return;
            }
            if (seqId === undefined && prevSeqId === undefined) {
                // OKX can omit sequencing fields; accept update when snapshot exists.
            } else if (seqId !== undefined && prevSeqId === undefined) {
                if (lastSeq !== undefined && seqId !== lastSeq + 1) {
                    this.recordGap(symbol, 'gap', { lastSeq, seqId, prevSeqId });
                    return;
                }
                this.orderbookSeq.set(symbol, seqId);
            } else if (seqId === undefined || prevSeqId === undefined) {
                this.recordGap(symbol, 'gap', { lastSeq, seqId, prevSeqId });
                return;
            } else {
                if (lastSeq !== undefined && prevSeqId !== lastSeq) {
                    this.recordGap(symbol, 'gap', { lastSeq, seqId, prevSeqId });
                    return;
                }
                this.orderbookSeq.set(symbol, seqId);
            }
            this.orderbookLastAppliedAt.set(symbol, exchangeTs);
        }

        if (isSnapshot) {
            this.clearResync(symbol);
            const evt: OrderbookL2SnapshotEvent = {
                symbol,
                streamId: this.streamId,
                updateId,
                exchangeTs,
                marketType: this.marketType ?? detectMarketType(instId),
                bids,
                asks,
                meta: createMeta('market', {
                    tsEvent: asTsMs(exchangeTs),
                    tsExchange: asTsMs(exchangeTs),
                    tsIngest: asTsMs(this.now()),
                    sequence: seqId !== undefined ? asSeq(seqId) : undefined,
                    streamId: this.streamId,
                }),
            } as OrderbookL2SnapshotEvent;
            this.bus.publish('market:orderbook_l2_snapshot', evt);
            this.bus.publish(
                'market:orderbook_snapshot_raw',
                mapOrderbookSnapshotRaw(this.venue, instId, obj.bids, obj.asks, exchangeTs, this.now(), seqId, this.marketType)
            );
            return;
        }

        const evt: OrderbookL2DeltaEvent = {
            symbol,
            streamId: this.streamId,
            updateId,
            exchangeTs,
            marketType: this.marketType ?? detectMarketType(instId),
            bids,
            asks,
            meta: createMeta('market', {
                tsEvent: asTsMs(exchangeTs),
                tsExchange: asTsMs(exchangeTs),
                tsIngest: asTsMs(this.now()),
                sequence: seqId !== undefined ? asSeq(seqId) : undefined,
                streamId: this.streamId,
            }),
        } as OrderbookL2DeltaEvent;
        this.bus.publish('market:orderbook_l2_delta', evt);
        this.bus.publish(
            'market:orderbook_delta_raw',
            mapOrderbookDeltaRaw(this.venue, instId, obj.bids, obj.asks, exchangeTs, this.now(), seqId, prevSeqId, undefined, this.marketType)
        );
    }

    private handleTicker(instId: string, rows: unknown[]): void {
        const payload = rows[0];
        if (!payload || typeof payload !== 'object') return;
        const obj = payload as Record<string, unknown>;
        const ts = toOptionalNumber(obj.ts);
        if (!ts) return;
        const evt: TickerEvent = {
            symbol: normalizeOkxSymbol(instId),
            streamId: this.streamId,
            marketType: this.marketType ?? detectMarketType(instId),
            lastPrice: obj.last !== undefined ? String(obj.last) : undefined,
            markPrice: obj.markPx !== undefined ? String(obj.markPx) : undefined,
            indexPrice: obj.idxPx !== undefined ? String(obj.idxPx) : undefined,
            highPrice24h: obj.high24h !== undefined ? String(obj.high24h) : undefined,
            lowPrice24h: obj.low24h !== undefined ? String(obj.low24h) : undefined,
            volume24h: obj.vol24h !== undefined ? String(obj.vol24h) : undefined,
            turnover24h: obj.volCcy24h !== undefined ? String(obj.volCcy24h) : undefined,
            exchangeTs: ts,
            meta: createMeta('market', {
                tsEvent: asTsMs(ts),
                tsExchange: asTsMs(ts),
                tsIngest: asTsMs(this.now()),
                streamId: this.streamId,
            }),
        } as TickerEvent;
        this.bus.publish('market:ticker', evt);

        const recvTs = this.now();
        if (evt.markPrice) {
            this.bus.publish('market:mark_price_raw', mapMarkPriceRaw(this.venue, instId, evt.markPrice, ts, recvTs, this.marketType));
        }
        if (evt.indexPrice) {
            this.bus.publish('market:index_price_raw', mapIndexPriceRaw(this.venue, instId, evt.indexPrice, ts, recvTs, this.marketType));
        }
    }

    private handleLiquidations(instId: string, rows: unknown[]): void {
        if (!this.supportsLiquidations) return;
        for (const row of rows) {
            if (!row || typeof row !== 'object') continue;
            const obj = row as Record<string, unknown>;
            const price = toOptionalNumber(obj.px ?? obj.price);
            const size = toOptionalNumber(obj.sz ?? obj.size);
            const sideRaw = obj.side ? String(obj.side).toLowerCase() : undefined;
            const side = sideRaw === 'buy' ? 'Buy' : sideRaw === 'sell' ? 'Sell' : undefined;
            const ts = toOptionalNumber(obj.ts);
            const evt: LiquidationEvent = {
                symbol: normalizeOkxSymbol(instId),
                streamId: this.streamId,
                side,
                price,
                size,
                notionalUsd: price !== undefined && size !== undefined ? price * size : undefined,
                exchangeTs: ts,
                marketType: this.marketType ?? detectMarketType(instId),
                meta: createMeta('market', {
                    tsEvent: asTsMs(ts ?? this.now()),
                    tsExchange: ts !== undefined ? asTsMs(ts) : undefined,
                    tsIngest: asTsMs(this.now()),
                    streamId: this.streamId,
                }),
            } as LiquidationEvent;
            this.bus.publish('market:liquidation', evt);

            if (ts !== undefined) {
                const rawEvt = mapLiquidationRaw(
                    this.venue,
                    instId,
                    ts,
                    this.now(),
                    { side: side?.toLowerCase(), price, size, notionalUsd: evt.notionalUsd },
                    this.marketType
                );
                this.bus.publish('market:liquidation_raw', rawEvt);
            }
        }
    }

    private startPing(): void {
        if (this.pingTimer) return;
        this.pingTimer = setInterval(() => {
            if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
            this.sendJson('ping');
        }, this.pingIntervalMs);
    }

    private stopPing(): void {
        if (this.pingTimer) {
            clearInterval(this.pingTimer);
            this.pingTimer = null;
        }
    }

    private scheduleStableReset(): void {
        this.clearStableReset();
        this.stableTimer = setTimeout(() => {
            this.reconnectAttempts = 0;
        }, this.backoffResetMs);
        this.stableTimer.unref?.();
    }

    private clearStableReset(): void {
        if (this.stableTimer) {
            clearTimeout(this.stableTimer);
            this.stableTimer = null;
        }
    }

    getLastReconnectDelayMs(): number | undefined {
        return this.lastReconnectDelayMs;
    }

    isAlive(): boolean {
        return Boolean(this.socket && this.socket.readyState === WebSocket.OPEN);
    }

    private requestResync(
        symbol: string,
        reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown',
        details?: Record<string, unknown>
    ): void {
        if (this.state !== 'open' && this.socket) {
            this.markPendingResync(symbol, reason, details);
            return;
        }
        const now = this.now();
        const inFlightUntil = this.resyncInFlightUntil.get(symbol) ?? 0;
        if (now < inFlightUntil) return;
        const lastResyncTs = this.lastResyncAt.get(symbol) ?? 0;
        if (now - lastResyncTs < this.resyncCooldownMs) return;
        this.resyncInFlightUntil.set(symbol, now + this.resyncCooldownMs);
        this.lastResyncAt.set(symbol, now);
        this.orderbookSeq.delete(symbol);
        this.orderbookSnapshotReady.delete(symbol);
        this.orderbookGapCount.delete(symbol);
        this.orderbookGapWindowStart.delete(symbol);
        this.bus.publish('market:resync_requested', {
            venue: this.venue,
            symbol,
            channel: 'orderbook',
            reason,
            streamId: this.streamId,
            details,
            meta: createMeta('market', { tsEvent: asTsMs(this.now()), tsIngest: asTsMs(this.now()) }),
        });
    }

    private markPendingResync(symbol: string, reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown', details?: Record<string, unknown>): void {
        if (this.orderbookPendingResync.has(symbol)) return;
        this.orderbookPendingResync.add(symbol);
        const now = this.now();
        const windowKey = `${symbol}:${reason}`;
        const lastTs = this.lastResyncAt.get(windowKey) ?? 0;
        if (now - lastTs < this.resyncPendingMaxMs) return;
        this.lastResyncAt.set(windowKey, now);
        this.requestResync(symbol, reason, details);
    }

    private recordGap(
        symbol: string,
        reason: 'gap' | 'out_of_order' | 'snapshot_missing' | 'sequence_reset' | 'crc_mismatch' | 'unknown',
        details?: Record<string, unknown>
    ): void {
        const now = this.now();
        const windowStart = this.orderbookGapWindowStart.get(symbol) ?? 0;
        if (windowStart === 0 || now - windowStart > this.resyncPendingMaxMs) {
            this.orderbookGapWindowStart.set(symbol, now);
            this.orderbookGapCount.set(symbol, 1);
        } else {
            const next = (this.orderbookGapCount.get(symbol) ?? 0) + 1;
            this.orderbookGapCount.set(symbol, next);
        }

        const count = this.orderbookGapCount.get(symbol) ?? 0;
        if (count < this.resyncMinGapCount) return;
        if (!this.orderbookSnapshotReady.has(symbol)) return;
        this.requestResync(symbol, reason, details);
    }

    private clearResync(symbol: string): void {
        this.resyncInFlightUntil.delete(symbol);
        this.orderbookPendingResync.delete(symbol);
        this.orderbookGapCount.delete(symbol);
        this.orderbookGapWindowStart.delete(symbol);
    }

    private scheduleReconnect(reason: string): void {
        if (this.isDisconnecting) return;
        if (this.reconnectTimer) return;
        this.reconnectAttempts += 1;
        const base = Math.min(this.reconnectMaxMs, this.reconnectBaseMs * Math.pow(2, this.reconnectAttempts - 1));
        const jitter = Math.floor(base * 0.2 * stableJitterFactor(`okx:${reason}`, this.reconnectAttempts));
        const delay = Math.min(this.reconnectMaxMs, base + jitter);
        this.lastReconnectDelayMs = delay;
        logger.warn(m('warn', `[OKXWS] reconnect in ${delay}ms (reason=${reason})`));
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            this.connect().catch((err) => {
                const msg = toErrorMessage(err);
                logger.warn(m('warn', `[OKXWS] reconnect failed: ${msg}`));
            });
        }, delay);
    }

    private sendJson(payload: unknown): void {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
        try {
            this.socket.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
        } catch {
            // ignore
        }
    }
}

const toErrorMessage = (err: unknown): string => {
    if (!err) return 'unknown error';
    if (err instanceof Error) return err.message;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
};

function normalizeOkxSymbol(instId: string): string {
    const trimmed = instId.trim().toUpperCase();
    return trimmed.replace(/-/g, '').replace('SWAP', '');
}

function buildSubKey(arg: OkxArg): string {
    return JSON.stringify({ channel: arg.channel, instId: arg.instId, instType: arg.instType });
}

function toOkxInstId(symbol: string, marketType?: MarketType): string | undefined {
    const raw = symbol.trim().toUpperCase();
    if (!raw) return undefined;
    if (raw.includes('-')) return raw;
    const match = raw.match(/^([A-Z0-9]+)(USDT|USDC|USD)$/);
    if (!match) return undefined;
    const base = `${match[1]}-${match[2]}`;
    if (marketType === 'spot') return base;
    return `${base}-SWAP`;
}

function toOkxInstType(marketType?: MarketType): OkxInstType {
    return marketType === 'spot' ? 'SPOT' : 'SWAP';
}

function detectMarketType(instId: string): MarketType {
    const upper = instId.toUpperCase();
    if (upper.includes('SWAP') || upper.includes('FUTURE')) return 'futures';
    return 'spot';
}

function parseLevels(raw: unknown): OrderbookLevel[] {
    if (!Array.isArray(raw)) return [];
    const levels: OrderbookLevel[] = [];
    for (const entry of raw) {
        if (!Array.isArray(entry)) continue;
        const price = toOptionalNumber(entry[0]);
        const size = toOptionalNumber(entry[1]);
        if (price === undefined || size === undefined) continue;
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

function toOkxCandleChannel(interval: KlineInterval): string | undefined {
    switch (interval) {
        case '1':
            return 'candle1m';
        case '3':
            return 'candle3m';
        case '5':
            return 'candle5m';
        case '15':
            return 'candle15m';
        case '30':
            return 'candle30m';
        case '60':
            return 'candle1H';
        case '120':
            return 'candle2H';
        case '240':
            return 'candle4H';
        case '360':
            return 'candle6H';
        case '720':
            return 'candle12H';
        case '1440':
            return 'candle1D';
        default:
            return undefined;
    }
}

function toKlineInterval(channel: string): KlineInterval | undefined {
    const map: Record<string, KlineInterval> = {
        candle1m: '1',
        candle3m: '3',
        candle5m: '5',
        candle15m: '15',
        candle30m: '30',
        candle1H: '60',
        candle2H: '120',
        candle4H: '240',
        candle6H: '360',
        candle12H: '720',
        candle1D: '1440',
    };
    return map[channel];
}

function intervalToTf(interval: KlineInterval): string {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return interval;
    if (minutes < 60) return `${minutes}m`;
    if (minutes % 1440 === 0) return `${minutes / 1440}d`;
    if (minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
}

function intervalToMs(interval: KlineInterval): number {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return 0;
    return minutes * 60_000;
}

function stableJitterFactor(seed: string, failures: number): number {
    const input = `${seed}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}

function readFlag(name: string, fallback: boolean): boolean {
    const raw = process.env[name];
    if (raw === undefined) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (normalized === '0' || normalized === 'false' || normalized === 'off') return false;
    if (normalized === '1' || normalized === 'true' || normalized === 'on') return true;
    return fallback;
}
