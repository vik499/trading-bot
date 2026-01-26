import https from 'node:https';
import { URL } from 'node:url';
import { logger } from '../../infra/logger';
import { m } from '../../core/logMarkers';
import {
    createMeta,
    eventBus,
    nowMs,
    type EventBus,
    type FundingRateEvent,
    type Kline,
    type KlineInterval,
    type MarketType,
    type OpenInterestEvent,
    type OrderbookLevel,
} from '../../core/events/EventBus';
import { normalizeSymbol } from '../../core/market/symbols';
import type { OpenInterestCollector } from '../../core/market/OpenInterestCollector';
import { mapFundingRaw, mapOpenInterestRaw } from '../normalizers/rawAdapters';

export interface BinanceFetchOpenInterestParams {
    symbol: string;
}

export interface BinanceFetchPremiumIndexParams {
    symbol: string;
}

export interface BinanceFetchKlinesParams {
    symbol: string;
    interval: KlineInterval;
    limit?: number;
    startTime?: number;
    endTime?: number;
    sinceTs?: number;
}

export interface BinanceFetchDepthParams {
    symbol: string;
    limit?: number;
}

export interface BinanceRequestMeta {
    url: string;
    endpoint: string;
    params: Record<string, string>;
    status: number;
    statusText: string;
    requestId?: string;
    rateLimit?: Record<string, string>;
    listLength?: number;
}

export class BinanceRestError extends Error {
    readonly details?: Record<string, unknown>;
    readonly originalError?: unknown;

    constructor(message: string, details?: Record<string, unknown>, originalError?: unknown) {
        super(message);
        this.name = 'BinanceRestError';
        this.details = details;
        this.originalError = originalError;
    }
}

interface HttpJsonResult {
    ok: boolean;
    status: number;
    statusText: string;
    data: unknown;
    headers: Record<string, string>;
}

interface MarkPriceState {
    price: number;
    ts: number;
}

interface BinanceSharedState {
    markPriceBySymbol: Map<string, MarkPriceState>;
}

const DEFAULT_BASE_URL = 'https://fapi.binance.com';
const DEFAULT_STREAM_ID = 'binance.usdm.public';

export class BinancePublicRestClient {
    public readonly streamId = DEFAULT_STREAM_ID;
    public readonly marketType: MarketType = 'futures';

    constructor(private readonly baseUrl = DEFAULT_BASE_URL) {}

    async fetchOpenInterest(params: BinanceFetchOpenInterestParams & { signal?: AbortSignal }): Promise<{ openInterest: number; ts: number }> {
        const url = this.buildOpenInterestUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BinanceRestError('Binance REST open interest failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new BinanceRestError('Binance REST open interest response is not an object', { ...responseMeta });
        }

        const openInterest = toNumber(payload.openInterest ?? payload.open_interest, 'openInterest');
        const ts = toNumber(payload.time ?? payload.timestamp ?? payload.ts, 'time');
        return { openInterest, ts };
    }

    async fetchPremiumIndex(
        params: BinanceFetchPremiumIndexParams & { signal?: AbortSignal }
    ): Promise<{ fundingRate: number; ts: number; nextFundingTs?: number; markPrice?: number; indexPrice?: number }> {
        const url = this.buildPremiumIndexUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BinanceRestError('Binance REST premium index failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (Array.isArray(payload)) {
            if (payload.length === 0) {
                throw new BinanceRestError('Binance REST premium index response is empty', { ...responseMeta });
            }
            throw new BinanceRestError('Binance REST premium index response is an array (symbol required)', {
                ...responseMeta,
            });
        }

        if (!isObject(payload)) {
            throw new BinanceRestError('Binance REST premium index response is not an object', { ...responseMeta });
        }

        const fundingRate = toNumber(payload.lastFundingRate ?? payload.last_funding_rate, 'lastFundingRate');
        const ts = toNumber(payload.time ?? payload.timestamp ?? payload.ts, 'time');
        const nextFundingTs = toOptionalNumber(payload.nextFundingTime ?? payload.next_funding_time);
        const markPrice = toOptionalNumber(payload.markPrice ?? payload.mark_price);
        const indexPrice = toOptionalNumber(payload.indexPrice ?? payload.index_price);

        return { fundingRate, ts, nextFundingTs, markPrice, indexPrice };
    }

    async fetchKlines(params: BinanceFetchKlinesParams & { signal?: AbortSignal }): Promise<{ klines: Kline[]; meta: BinanceRequestMeta }> {
        const url = this.buildKlinesUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BinanceRestError('Binance REST klines failed', { ...responseMeta });
        }

        const payload = result.data;
        if (!Array.isArray(payload)) {
            throw new BinanceRestError('Binance REST klines response is not an array', { ...responseMeta });
        }

        const klines = payload.map((row) => parseKlineRow(row, params.symbol, params.interval));
        return {
            klines,
            meta: {
                ...responseMeta,
                listLength: klines.length,
            },
        };
    }

    async fetchDepthSnapshot(params: BinanceFetchDepthParams & { signal?: AbortSignal }): Promise<{ lastUpdateId: number; bids: OrderbookLevel[]; asks: OrderbookLevel[] }> {
        const url = this.buildDepthUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BinanceRestError('Binance REST depth failed', { ...responseMeta });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new BinanceRestError('Binance REST depth response is not an object', { ...responseMeta });
        }

        const lastUpdateId = toNumber(payload.lastUpdateId ?? payload.last_update_id, 'lastUpdateId');
        const bids = parseDepthLevels(payload.bids, 'bids');
        const asks = parseDepthLevels(payload.asks, 'asks');
        return { lastUpdateId, bids, asks };
    }

    private buildOpenInterestUrl(params: BinanceFetchOpenInterestParams): string {
        const url = new URL('/fapi/v1/openInterest', this.baseUrl);
        url.searchParams.set('symbol', params.symbol);
        return url.toString();
    }

    private buildPremiumIndexUrl(params: BinanceFetchPremiumIndexParams): string {
        const url = new URL('/fapi/v1/premiumIndex', this.baseUrl);
        url.searchParams.set('symbol', params.symbol);
        return url.toString();
    }

    private buildKlinesUrl(params: BinanceFetchKlinesParams): string {
        const url = new URL('/fapi/v1/klines', this.baseUrl);
        const interval = toBinanceInterval(params.interval);
        if (!interval) {
            throw new BinanceRestError('Unsupported kline interval', { interval: params.interval });
        }
        url.searchParams.set('symbol', params.symbol);
        url.searchParams.set('interval', interval);
        if (params.limit !== undefined) url.searchParams.set('limit', String(params.limit));
        const startTime = params.startTime ?? params.sinceTs;
        if (startTime !== undefined) url.searchParams.set('startTime', String(startTime));
        if (params.endTime !== undefined) url.searchParams.set('endTime', String(params.endTime));
        return url.toString();
    }

    private buildDepthUrl(params: BinanceFetchDepthParams): string {
        const url = new URL('/fapi/v1/depth', this.baseUrl);
        url.searchParams.set('symbol', params.symbol);
        if (params.limit !== undefined) url.searchParams.set('limit', String(params.limit));
        return url.toString();
    }
}

export interface BinanceOpenInterestCollectorOptions {
    oiIntervalMs?: number;
    streamId?: string;
    markPriceMaxAgeMs?: number;
    now?: () => number;
}

export class BinanceOpenInterestCollector implements OpenInterestCollector {
    private readonly restClient: BinancePublicRestClient;
    private readonly bus: EventBus;
    private readonly oiIntervalMs: number;
    private readonly streamId: string;
    private readonly markPriceMaxAgeMs: number;
    private readonly now: () => number;
    private readonly shared: BinanceSharedState;
    private readonly timers = new Map<string, NodeJS.Timeout>();
    private readonly inFlight = new Set<string>();
    private readonly lastEmittedTs = new Map<string, number>();
    private readonly lastErrorLogAt = new Map<string, number>();
    private readonly backoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly abortControllers = new Map<string, AbortController>();

    constructor(
        restClient: BinancePublicRestClient = new BinancePublicRestClient(),
        bus: EventBus = eventBus,
        shared: BinanceSharedState = { markPriceBySymbol: new Map() },
        opts: BinanceOpenInterestCollectorOptions = {}
    ) {
        this.restClient = restClient;
        this.bus = bus;
        this.shared = shared;
        this.oiIntervalMs = Math.max(10_000, opts.oiIntervalMs ?? 30_000);
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.markPriceMaxAgeMs = Math.max(10_000, opts.markPriceMaxAgeMs ?? 120_000);
        this.now = opts.now ?? nowMs;
    }

    startForSymbol(symbol: string): void {
        const key = normalizeSymbol(symbol);
        if (!key) return;
        if (!this.timers.has(key)) {
            this.pollOpenInterest(key);
            const timer = setInterval(() => this.pollOpenInterest(key), this.oiIntervalMs);
            this.timers.set(key, timer);
        }
    }

    stop(): void {
        for (const timer of this.timers.values()) {
            clearInterval(timer);
        }
        this.timers.clear();
        for (const controller of this.abortControllers.values()) {
            controller.abort();
        }
        this.abortControllers.clear();
        this.inFlight.clear();
        this.lastEmittedTs.clear();
        this.backoffState.clear();
    }

    private async pollOpenInterest(symbol: string): Promise<void> {
        if (this.inFlight.has(symbol)) return;
        if (!this.shouldPoll(symbol)) return;
        this.inFlight.add(symbol);
        const controller = new AbortController();
        this.abortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchOpenInterest({ symbol, signal: controller.signal });
            const meta = createMeta('binance', { ts: data.ts });

            if (this.lastEmittedTs.get(symbol) === data.ts) {
                this.resetBackoff(symbol);
                return;
            }

            const payload: OpenInterestEvent = {
                symbol,
                streamId: this.streamId,
                openInterest: data.openInterest,
                openInterestUnit: 'contracts',
                openInterestValueUsd: undefined,
                exchangeTs: data.ts,
                marketType: this.restClient.marketType,
                meta,
            };
            this.bus.publish('market:oi', payload);
            this.bus.publish(
                'market:open_interest_raw',
                mapOpenInterestRaw('binance', symbol, data.openInterest, data.ts, this.now(), undefined, this.restClient.marketType)
            );
            this.lastEmittedTs.set(symbol, data.ts);
            this.resetBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            this.logErrorThrottled(
                `oi:${symbol}`,
                `[BinanceREST] open interest failed for ${symbol}: ${(err as Error).message}`,
                err
            );
            this.bumpBackoff(symbol);
        } finally {
            this.abortControllers.delete(symbol);
            this.inFlight.delete(symbol);
        }
    }

    private shouldPoll(symbol: string): boolean {
        const state = this.backoffState.get(symbol);
        if (!state) return true;
        return this.now() >= state.nextAllowedTs;
    }

    private resetBackoff(symbol: string): void {
        this.backoffState.delete(symbol);
    }

    private bumpBackoff(symbol: string): void {
        const existing = this.backoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        this.backoffState.set(symbol, {
            failures,
            nextAllowedTs: this.now() + backoffMs,
        });
    }

    private computeBackoffMs(symbol: string, failures: number): number {
        const exp = Math.min(6, failures);
        const base = this.oiIntervalMs;
        const backoff = Math.min(300_000, base * Math.pow(2, exp));
        const jitter = Math.floor(backoff * 0.1 * stableJitterFactor(symbol, failures));
        return backoff + jitter;
    }

    private logErrorThrottled(key: string, message: string, err?: unknown): void {
        const now = this.now();
        const last = this.lastErrorLogAt.get(key) ?? 0;
        if (now - last < 30_000) return;
        this.lastErrorLogAt.set(key, now);
        const details = formatRestError(err);
        const full = details ? `${message} ${details}` : message;
        logger.warn(m('warn', full));
    }
}

export interface BinanceFundingCollectorOptions {
    fundingIntervalMs?: number;
    streamId?: string;
    now?: () => number;
}

export class BinanceFundingCollector implements OpenInterestCollector {
    private readonly restClient: BinancePublicRestClient;
    private readonly bus: EventBus;
    private readonly fundingIntervalMs: number;
    private readonly streamId: string;
    private readonly shared: BinanceSharedState;
    private readonly now: () => number;
    private readonly timers = new Map<string, NodeJS.Timeout>();
    private readonly inFlight = new Set<string>();
    private readonly lastEmittedTs = new Map<string, number>();
    private readonly lastErrorLogAt = new Map<string, number>();
    private readonly backoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly abortControllers = new Map<string, AbortController>();

    constructor(
        restClient: BinancePublicRestClient = new BinancePublicRestClient(),
        bus: EventBus = eventBus,
        shared: BinanceSharedState = { markPriceBySymbol: new Map() },
        opts: BinanceFundingCollectorOptions = {}
    ) {
        this.restClient = restClient;
        this.bus = bus;
        this.shared = shared;
        this.fundingIntervalMs = Math.max(10_000, opts.fundingIntervalMs ?? 60_000);
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.now = opts.now ?? nowMs;
    }

    startForSymbol(symbol: string): void {
        const key = normalizeSymbol(symbol);
        if (!key) return;
        if (!this.timers.has(key)) {
            this.pollFunding(key);
            const timer = setInterval(() => this.pollFunding(key), this.fundingIntervalMs);
            this.timers.set(key, timer);
        }
    }

    stop(): void {
        for (const timer of this.timers.values()) {
            clearInterval(timer);
        }
        this.timers.clear();
        for (const controller of this.abortControllers.values()) {
            controller.abort();
        }
        this.abortControllers.clear();
        this.inFlight.clear();
        this.lastEmittedTs.clear();
        this.backoffState.clear();
    }

    private async pollFunding(symbol: string): Promise<void> {
        if (this.inFlight.has(symbol)) return;
        if (!this.shouldPoll(symbol)) return;
        this.inFlight.add(symbol);
        const controller = new AbortController();
        this.abortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchPremiumIndex({ symbol, signal: controller.signal });
            const meta = createMeta('binance', { ts: data.ts });

            if (this.lastEmittedTs.get(symbol) === data.ts) {
                this.resetBackoff(symbol);
                return;
            }

            if (data.markPrice !== undefined) {
                this.shared.markPriceBySymbol.set(symbol, { price: data.markPrice, ts: data.ts });
            }

            const payload: FundingRateEvent = {
                symbol,
                streamId: this.streamId,
                fundingRate: data.fundingRate,
                exchangeTs: data.ts,
                nextFundingTs: data.nextFundingTs,
                marketType: this.restClient.marketType,
                meta,
            };
            this.bus.publish('market:funding', payload);
            this.bus.publish(
                'market:funding_raw',
                mapFundingRaw('binance', symbol, data.fundingRate, data.ts, this.now(), data.nextFundingTs, this.restClient.marketType)
            );
            this.lastEmittedTs.set(symbol, data.ts);
            this.resetBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            this.logErrorThrottled(
                `funding:${symbol}`,
                `[BinanceREST] funding failed for ${symbol}: ${(err as Error).message}`,
                err
            );
            this.bumpBackoff(symbol);
        } finally {
            this.abortControllers.delete(symbol);
            this.inFlight.delete(symbol);
        }
    }

    private shouldPoll(symbol: string): boolean {
        const state = this.backoffState.get(symbol);
        if (!state) return true;
        return this.now() >= state.nextAllowedTs;
    }

    private resetBackoff(symbol: string): void {
        this.backoffState.delete(symbol);
    }

    private bumpBackoff(symbol: string): void {
        const existing = this.backoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        this.backoffState.set(symbol, {
            failures,
            nextAllowedTs: this.now() + backoffMs,
        });
    }

    private computeBackoffMs(symbol: string, failures: number): number {
        const exp = Math.min(6, failures);
        const base = this.fundingIntervalMs;
        const backoff = Math.min(300_000, base * Math.pow(2, exp));
        const jitter = Math.floor(backoff * 0.1 * stableJitterFactor(symbol, failures));
        return backoff + jitter;
    }

    private logErrorThrottled(key: string, message: string, err?: unknown): void {
        const now = this.now();
        const last = this.lastErrorLogAt.get(key) ?? 0;
        if (now - last < 30_000) return;
        this.lastErrorLogAt.set(key, now);
        const details = formatRestError(err);
        const full = details ? `${message} ${details}` : message;
        logger.warn(m('warn', full));
    }
}

export interface BinanceDerivativesPollerOptions {
    oiIntervalMs?: number;
    fundingIntervalMs?: number;
    streamId?: string;
    markPriceMaxAgeMs?: number;
    now?: () => number;
}

export class BinanceDerivativesRestPoller implements OpenInterestCollector {
    private readonly oiCollector: BinanceOpenInterestCollector;
    private readonly fundingCollector: BinanceFundingCollector;

    constructor(
        restClient: BinancePublicRestClient = new BinancePublicRestClient(),
        bus: EventBus = eventBus,
        opts: BinanceDerivativesPollerOptions = {}
    ) {
        const shared: BinanceSharedState = { markPriceBySymbol: new Map() };
        this.oiCollector = new BinanceOpenInterestCollector(restClient, bus, shared, {
            oiIntervalMs: opts.oiIntervalMs,
            streamId: opts.streamId,
            markPriceMaxAgeMs: opts.markPriceMaxAgeMs,
            now: opts.now,
        });
        this.fundingCollector = new BinanceFundingCollector(restClient, bus, shared, {
            fundingIntervalMs: opts.fundingIntervalMs,
            streamId: opts.streamId,
            now: opts.now,
        });
    }

    startForSymbol(symbol: string): void {
        this.oiCollector.startForSymbol(symbol);
        this.fundingCollector.startForSymbol(symbol);
    }

    stop(): void {
        this.oiCollector.stop();
        this.fundingCollector.stop();
    }
}

async function fetchJson(url: string, signal?: AbortSignal): Promise<HttpJsonResult> {
    if (typeof globalThis.fetch === 'function') {
        const response = await globalThis.fetch(url, signal ? { signal } : undefined);
        const data = await response.json().catch(() => undefined);
        const headers: Record<string, string> = {};
        response.headers.forEach((value, key) => {
            headers[key.toLowerCase()] = value;
        });
        return {
            ok: response.ok,
            status: response.status,
            statusText: response.statusText,
            data,
            headers,
        };
    }

    return httpsJson(url, signal);
}

function httpsJson(url: string, signal?: AbortSignal): Promise<HttpJsonResult> {
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(new BinanceRestError('Binance REST aborted', { url }, new Error('AbortError')));
            return;
        }
        const request = https.get(url, (res) => {
            const status = res.statusCode ?? 0;
            const statusText = res.statusMessage ?? '';
            const ok = status >= 200 && status < 300;
            let body = '';

            res.setEncoding('utf8');
            res.on('data', (chunk) => {
                body += chunk;
            });
            res.on('end', () => {
                try {
                    const data = body ? (JSON.parse(body) as unknown) : undefined;
                    const headers = normalizeHeaders(res.headers);
                    resolve({ ok, status, statusText, data, headers });
                } catch (error) {
                    reject(new BinanceRestError('Binance REST invalid JSON', { status, statusText, url }, error));
                }
            });
        });

        request.on('error', (error) => {
            reject(new BinanceRestError('Binance REST request error', { url }, error));
        });

        if (signal) {
            const onAbort = () => {
                request.destroy(new Error('AbortError'));
                reject(new BinanceRestError('Binance REST aborted', { url }, new Error('AbortError')));
            };
            signal.addEventListener('abort', onAbort, { once: true });
            request.on('close', () => signal.removeEventListener('abort', onAbort));
        }
    });
}

function isAbortError(err: unknown): boolean {
    if (err instanceof BinanceRestError) {
        const original = err.originalError as { name?: string; message?: string; code?: string } | undefined;
        if (original?.name === 'AbortError') return true;
        if (original?.code === 'ABORT_ERR') return true;
    }
    if (err instanceof Error && err.name === 'AbortError') return true;
    return false;
}

function toNumber(value: unknown, label: string): number {
    const num = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(num)) {
        throw new BinanceRestError('Binance REST invalid number', { label, value });
    }
    return num;
}

function toOptionalNumber(value: unknown): number | undefined {
    if (value === undefined || value === null) return undefined;
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : undefined;
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
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

function intervalToTf(interval: KlineInterval): string {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return interval;
    if (minutes < 60) return `${minutes}m`;
    if (minutes % 1440 === 0) return `${minutes / 1440}d`;
    if (minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
}

function parseKlineRow(row: unknown, symbol: string, interval: KlineInterval): Kline {
    if (!Array.isArray(row) || row.length < 6) {
        throw new BinanceRestError('Binance REST kline row invalid', { rowPreview: JSON.stringify(row) });
    }
    const startTs = toNumber(row[0], 'openTime');
    const endTs = toNumber(row[6] ?? row[0], 'closeTime');
    const open = toNumber(row[1], 'open');
    const high = toNumber(row[2], 'high');
    const low = toNumber(row[3], 'low');
    const close = toNumber(row[4], 'close');
    const volume = toNumber(row[5], 'volume');
    return {
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
    };
}

function parseDepthLevels(raw: unknown, label: string): OrderbookLevel[] {
    if (!Array.isArray(raw)) {
        throw new BinanceRestError('Binance REST depth levels invalid', { label });
    }
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

function stableJitterFactor(symbol: string, failures: number): number {
    const input = `${symbol}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}

function buildRequestMeta(url: string): Pick<BinanceRequestMeta, 'url' | 'endpoint' | 'params'> {
    const parsed = new URL(url);
    const params: Record<string, string> = {};
    parsed.searchParams.forEach((value, key) => {
        params[key] = value;
    });
    return {
        url,
        endpoint: parsed.pathname,
        params,
    };
}

function buildResponseMeta(result: HttpJsonResult, request: Pick<BinanceRequestMeta, 'url' | 'endpoint' | 'params'>): BinanceRequestMeta {
    const requestId = extractRequestId(result.headers);
    const rateLimit = extractRateLimit(result.headers);
    return {
        ...request,
        status: result.status,
        statusText: result.statusText,
        requestId,
        rateLimit,
    };
}

function normalizeHeaders(headers: Record<string, string | string[] | undefined>): Record<string, string> {
    const normalized: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers)) {
        if (value === undefined) continue;
        const normalizedKey = key.toLowerCase();
        normalized[normalizedKey] = Array.isArray(value) ? value.join(',') : value;
    }
    return normalized;
}

function extractRequestId(headers: Record<string, string>): string | undefined {
    const candidates = ['x-mbx-uuid', 'x-mbx-trace-id', 'x-request-id', 'request-id'];
    for (const key of candidates) {
        const value = headers[key];
        if (value) return value;
    }
    return undefined;
}

function extractRateLimit(headers: Record<string, string>): Record<string, string> | undefined {
    const keys = [
        'x-mbx-used-weight',
        'x-mbx-used-weight-1m',
        'x-mbx-used-weight-1d',
        'x-mbx-order-count-1s',
        'x-mbx-order-count-10s',
        'x-mbx-order-count-1m',
        'retry-after',
    ];
    const rateLimit: Record<string, string> = {};
    for (const key of keys) {
        const value = headers[key];
        if (value) rateLimit[key] = value;
    }
    return Object.keys(rateLimit).length ? rateLimit : undefined;
}

function formatRestError(err: unknown): string | undefined {
    if (!(err instanceof BinanceRestError)) return undefined;
    const details = err.details;
    if (!details || typeof details !== 'object') return undefined;

    const parts: string[] = [];
    pushDetail(parts, 'status', details.status);
    pushDetail(parts, 'statusText', details.statusText);
    pushDetail(parts, 'endpoint', details.endpoint);
    pushDetail(parts, 'params', details.params);
    pushDetail(parts, 'requestId', details.requestId);
    pushDetail(parts, 'rateLimit', details.rateLimit);

    return parts.length ? `details=${parts.join(' ')}` : undefined;
}

function pushDetail(parts: string[], key: string, value: unknown): void {
    if (value === undefined || value === null) return;
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        parts.push(`${key}=${value}`);
        return;
    }
    parts.push(`${key}=${JSON.stringify(value)}`);
}
