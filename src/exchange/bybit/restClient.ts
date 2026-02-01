import https from 'node:https';
import { URL } from 'node:url';
import { logger } from '../../infra/logger';
import { m } from '../../core/logMarkers';
import {
    asTsMs,
    createMeta,
    eventBus,
    nowMs,
    type EventBus,
    type FundingRateEvent,
    type Kline,
    type KlineInterval,
    type KnownMarketType,
    type MarketType,
    type OpenInterestEvent,
} from '../../core/events/EventBus';
import type { OpenInterestCollector } from '../../core/market/OpenInterestCollector';
import { mapFundingRaw, mapOpenInterestRaw } from '../normalizers/rawAdapters';

export interface FetchKlinesParams {
    symbol: string;
    interval: KlineInterval;
    limit?: number;
    sinceTs?: number;
}

export interface FetchOpenInterestParams {
    symbol: string;
    intervalTime?: string;
}

export interface FetchFundingRateParams {
    symbol: string;
}

export interface KlineFetchMeta {
    url: string;
    endpoint: string;
    params: Record<string, string>;
    status: number;
    statusText: string;
    retCode?: number;
    retMsg?: string;
    requestId?: string;
    rateLimit?: Record<string, string>;
    listLength?: number;
}

export interface KlineFetchResult {
    klines: Kline[];
    meta: KlineFetchMeta;
}

export class BybitRestError extends Error {
    readonly details?: Record<string, unknown>;
    readonly originalError?: unknown;

    constructor(message: string, details?: Record<string, unknown>, originalError?: unknown) {
        super(message);
        this.name = 'BybitRestError';
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

const DEFAULT_BASE_URL = 'https://api.bybit.com';
const DEFAULT_CATEGORY = 'linear';
const DEFAULT_STREAM_ID = 'bybit.public.linear.v5';
const DEFAULT_OI_INTERVAL_TIME = '5min';

export class BybitPublicRestClient {
    public readonly streamId = DEFAULT_STREAM_ID;
    public readonly marketType: MarketType = 'futures';

    constructor(private readonly baseUrl = DEFAULT_BASE_URL) {}

    async fetchKlines(params: FetchKlinesParams & { signal?: AbortSignal }): Promise<KlineFetchResult> {
        const url = this.buildKlineUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BybitRestError('Bybit REST request failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new BybitRestError('Bybit REST response is not an object', { ...responseMeta });
        }

        const retCode = toNumber(payload.retCode, 'retCode');
        const retMsg = typeof payload.retMsg === 'string' ? payload.retMsg : 'unknown';
        if (retCode !== 0) {
            throw new BybitRestError('Bybit REST returned error', { ...responseMeta, retCode, retMsg });
        }

        const resultObj = payload.result;
        if (!isObject(resultObj)) {
            throw new BybitRestError('Bybit REST missing result', { ...responseMeta, retCode, retMsg });
        }

        const list = resultObj.list;
        if (!Array.isArray(list)) {
            throw new BybitRestError('Bybit REST kline list missing', { ...responseMeta, retCode, retMsg });
        }

        const intervalMs = intervalToMs(params.interval);
        const marketType = this.marketType ?? 'futures';
        if (marketType === 'unknown') {
            logger.warn(m('warn', `[BybitREST] klines skipped for ${params.symbol}: marketType=unknown`));
            return {
                klines: [],
                meta: {
                    ...responseMeta,
                    retCode,
                    retMsg,
                    listLength: 0,
                },
            };
        }
        const knownMarketType: KnownMarketType = marketType;
        const klines = list.map((row) =>
            parseKlineRow(row, params.symbol, params.interval, intervalMs, this.streamId, knownMarketType)
        );
        return {
            klines,
            meta: {
                ...responseMeta,
                retCode,
                retMsg,
                listLength: list.length,
            },
        };
    }

    async fetchOpenInterest(params: FetchOpenInterestParams & { signal?: AbortSignal }): Promise<{ openInterest: number; ts: number }> {
        const url = this.buildOpenInterestUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BybitRestError('Bybit REST open interest failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new BybitRestError('Bybit REST open interest response is not an object', { ...responseMeta });
        }

        const retCode = toNumber(payload.retCode, 'retCode');
        const retMsg = typeof payload.retMsg === 'string' ? payload.retMsg : 'unknown';
        if (retCode !== 0) {
            throw new BybitRestError('Bybit REST open interest returned error', { ...responseMeta, retCode, retMsg });
        }

        const resultObj = payload.result;
        if (!isObject(resultObj)) {
            throw new BybitRestError('Bybit REST open interest missing result', { ...responseMeta, retCode, retMsg });
        }

        const list = resultObj.list;
        if (!Array.isArray(list) || list.length === 0) {
            throw new BybitRestError('Bybit REST open interest list missing', {
                ...responseMeta,
                retCode,
                retMsg,
                listLength: Array.isArray(list) ? list.length : undefined,
            });
        }

        return parseOpenInterestRow(list[0]);
    }

    async fetchFundingRate(params: FetchFundingRateParams & { signal?: AbortSignal }): Promise<{ fundingRate: number; ts: number; nextFundingTs?: number }> {
        const url = this.buildFundingRateUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new BybitRestError('Bybit REST funding rate failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new BybitRestError('Bybit REST funding response is not an object', { ...responseMeta });
        }

        const retCode = toNumber(payload.retCode, 'retCode');
        const retMsg = typeof payload.retMsg === 'string' ? payload.retMsg : 'unknown';
        if (retCode !== 0) {
            throw new BybitRestError('Bybit REST funding returned error', { ...responseMeta, retCode, retMsg });
        }

        const resultObj = payload.result;
        if (!isObject(resultObj)) {
            throw new BybitRestError('Bybit REST funding missing result', { ...responseMeta, retCode, retMsg });
        }

        const list = resultObj.list;
        if (!Array.isArray(list) || list.length === 0) {
            throw new BybitRestError('Bybit REST funding list missing', {
                ...responseMeta,
                retCode,
                retMsg,
                listLength: Array.isArray(list) ? list.length : undefined,
            });
        }

        return parseFundingRow(list[0]);
    }

    private buildKlineUrl(params: FetchKlinesParams): string {
        const limit = Math.max(1, Math.min(1000, params.limit ?? 200));
        const url = new URL('/v5/market/kline', this.baseUrl);
        url.searchParams.set('category', DEFAULT_CATEGORY);
        url.searchParams.set('symbol', params.symbol);
        url.searchParams.set('interval', intervalToApi(params.interval));
        url.searchParams.set('limit', String(limit));
        if (typeof params.sinceTs === 'number' && Number.isFinite(params.sinceTs)) {
            url.searchParams.set('start', String(Math.floor(params.sinceTs)));
        }
        return url.toString();
    }

    private buildOpenInterestUrl(params: FetchOpenInterestParams): string {
        const url = new URL('/v5/market/open-interest', this.baseUrl);
        url.searchParams.set('category', DEFAULT_CATEGORY);
        url.searchParams.set('symbol', params.symbol);
        url.searchParams.set('intervalTime', params.intervalTime ?? DEFAULT_OI_INTERVAL_TIME);
        url.searchParams.set('limit', '1');
        return url.toString();
    }

    private buildFundingRateUrl(params: FetchFundingRateParams): string {
        const url = new URL('/v5/market/funding/history', this.baseUrl);
        url.searchParams.set('category', DEFAULT_CATEGORY);
        url.searchParams.set('symbol', params.symbol);
        url.searchParams.set('limit', '1');
        return url.toString();
    }
}

function parseKlineRow(
    row: unknown,
    symbol: string,
    interval: KlineInterval,
    intervalMs: number,
    streamId: string,
    marketType: KnownMarketType
): Kline {
    if (!Array.isArray(row) || row.length < 6) {
        throw new BybitRestError('Bybit REST kline row invalid', { rowPreview: JSON.stringify(row) });
    }

    const startTs = toNumber(row[0], 'startTs');
    const open = toNumber(row[1], 'open');
    const high = toNumber(row[2], 'high');
    const low = toNumber(row[3], 'low');
    const close = toNumber(row[4], 'close');
    const volume = toNumber(row[5], 'volume');
    const endTs = startTs + intervalMs;
    const tf = intervalToTf(interval);

    return { symbol, streamId, marketType, interval, tf, startTs, endTs, open, high, low, close, volume };
}

function parseOpenInterestRow(row: unknown): { openInterest: number; ts: number } {
    if (!isObject(row)) {
        throw new BybitRestError('Bybit REST open interest row invalid', { rowPreview: JSON.stringify(row) });
    }
    const openInterest = toNumber(row.openInterest ?? row.open_interest, 'openInterest');
    const ts = toNumber(row.timestamp ?? row.ts ?? row.time, 'timestamp');
    return { openInterest, ts };
}

function parseFundingRow(row: unknown): { fundingRate: number; ts: number; nextFundingTs?: number } {
    if (!isObject(row)) {
        throw new BybitRestError('Bybit REST funding row invalid', { rowPreview: JSON.stringify(row) });
    }
    const fundingRate = toNumber(row.fundingRate ?? row.funding_rate, 'fundingRate');
    const ts = toNumber(row.fundingRateTimestamp ?? row.fundingRateTs ?? row.timestamp ?? row.ts ?? row.time, 'timestamp');
    const nextFundingTs = toOptionalNumber(row.nextFundingTime ?? row.next_funding_time);
    return { fundingRate, ts, nextFundingTs };
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
            reject(new BybitRestError('Bybit REST aborted', { url }, new Error('AbortError')));
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
                    reject(new BybitRestError('Bybit REST invalid JSON', { status, statusText, url }, error));
                }
            });
        });

        request.on('error', (error) => {
            reject(new BybitRestError('Bybit REST request error', { url }, error));
        });

        if (signal) {
            const onAbort = () => {
                request.destroy(new Error('AbortError'));
                reject(new BybitRestError('Bybit REST aborted', { url }, new Error('AbortError')));
            };
            signal.addEventListener('abort', onAbort, { once: true });
            request.on('close', () => signal.removeEventListener('abort', onAbort));
        }
    });
}

function isAbortError(err: unknown): boolean {
    if (err instanceof BybitRestError) {
        const original = err.originalError as { name?: string; message?: string; code?: string } | undefined;
        if (original?.name === 'AbortError') return true;
        if (original?.code === 'ABORT_ERR') return true;
    }
    if (err instanceof Error && err.name === 'AbortError') return true;
    return false;
}

function intervalToMs(interval: KlineInterval): number {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) {
        throw new BybitRestError('Unsupported kline interval', { interval });
    }
    return minutes * 60_000;
}

function intervalToApi(interval: KlineInterval): string {
    if (interval === '1440') return 'D';
    return interval;
}

function intervalToTf(interval: KlineInterval): string {
    switch (interval) {
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
            return `${interval}m`;
    }
}

function stableJitterFactor(symbol: string, failures: number): number {
    const input = `${symbol}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}

function toNumber(value: unknown, label: string): number {
    const num = typeof value === 'number' ? value : Number(value);
    if (!Number.isFinite(num)) {
        throw new BybitRestError('Bybit REST invalid number', { label, value });
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

function buildRequestMeta(url: string): Pick<KlineFetchMeta, 'url' | 'endpoint' | 'params'> {
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

function buildResponseMeta(result: HttpJsonResult, request: Pick<KlineFetchMeta, 'url' | 'endpoint' | 'params'>): KlineFetchMeta {
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
    const candidates = [
        'x-bapi-trace-id',
        'x-bapi-request-id',
        'x-bapi-requestid',
        'x-request-id',
        'request-id',
    ];
    for (const key of candidates) {
        const value = headers[key];
        if (value) return value;
    }
    return undefined;
}

function extractRateLimit(headers: Record<string, string>): Record<string, string> | undefined {
    const keys = [
        'x-bapi-limit',
        'x-bapi-limit-status',
        'x-bapi-limit-reset',
        'x-ratelimit-limit',
        'x-ratelimit-remaining',
        'x-ratelimit-reset',
        'retry-after',
    ];
    const rateLimit: Record<string, string> = {};
    for (const key of keys) {
        const value = headers[key];
        if (value) rateLimit[key] = value;
    }
    return Object.keys(rateLimit).length ? rateLimit : undefined;
}

export interface DerivativesPollerOptions {
    oiIntervalMs?: number;
    fundingIntervalMs?: number;
    streamId?: string;
    oiIntervalTime?: string;
    now?: () => number;
}

export class BybitDerivativesRestPoller implements OpenInterestCollector {
    private readonly restClient: BybitPublicRestClient;
    private readonly bus: EventBus;
    private readonly oiIntervalMs: number;
    private readonly fundingIntervalMs: number;
    private readonly streamId: string;
    private readonly oiIntervalTime: string;
    private readonly now: () => number;
    private readonly oiTimers = new Map<string, NodeJS.Timeout>();
    private readonly fundingTimers = new Map<string, NodeJS.Timeout>();
    private readonly oiInFlight = new Set<string>();
    private readonly fundingInFlight = new Set<string>();
    private readonly lastErrorLogAt = new Map<string, number>();
    private readonly oiBackoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly fundingBackoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly oiAbortControllers = new Map<string, AbortController>();
    private readonly fundingAbortControllers = new Map<string, AbortController>();
    private readonly lastFundingTs = new Map<string, number>();

    constructor(
        restClient: BybitPublicRestClient = new BybitPublicRestClient(),
        bus: EventBus = eventBus,
        opts: DerivativesPollerOptions = {}
    ) {
        this.restClient = restClient;
        this.bus = bus;
        this.oiIntervalMs = Math.max(10_000, opts.oiIntervalMs ?? 30_000);
        this.fundingIntervalMs = Math.max(10_000, opts.fundingIntervalMs ?? 60_000);
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        const intervalTime = (opts.oiIntervalTime ?? DEFAULT_OI_INTERVAL_TIME).trim();
        this.oiIntervalTime = intervalTime || DEFAULT_OI_INTERVAL_TIME;
        this.now = opts.now ?? nowMs;
    }

    startForSymbol(symbol: string): void {
        const key = symbol.trim();
        if (!key) return;
        if (!this.oiTimers.has(key)) {
            this.pollOpenInterest(key);
            const timer = setInterval(() => this.pollOpenInterest(key), this.oiIntervalMs);
            this.oiTimers.set(key, timer);
        }
        if (!this.fundingTimers.has(key)) {
            this.pollFundingRate(key);
            const timer = setInterval(() => this.pollFundingRate(key), this.fundingIntervalMs);
            this.fundingTimers.set(key, timer);
        }
    }

    stop(): void {
        for (const timer of this.oiTimers.values()) {
            clearInterval(timer);
        }
        for (const timer of this.fundingTimers.values()) {
            clearInterval(timer);
        }
        this.oiTimers.clear();
        this.fundingTimers.clear();
        for (const controller of this.oiAbortControllers.values()) {
            controller.abort();
        }
        for (const controller of this.fundingAbortControllers.values()) {
            controller.abort();
        }
        this.oiAbortControllers.clear();
        this.fundingAbortControllers.clear();
        this.oiInFlight.clear();
        this.fundingInFlight.clear();
        this.oiBackoffState.clear();
        this.fundingBackoffState.clear();
        this.lastFundingTs.clear();
    }

    private async pollOpenInterest(symbol: string): Promise<void> {
        if (this.oiInFlight.has(symbol)) return;
        if (!this.shouldPollOi(symbol)) return;
        this.oiInFlight.add(symbol);
        const controller = new AbortController();
        this.oiAbortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchOpenInterest({ symbol, intervalTime: this.oiIntervalTime, signal: controller.signal });
            const meta = createMeta('market', {
                tsEvent: asTsMs(data.ts),
                tsIngest: asTsMs(this.now()),
                tsExchange: asTsMs(data.ts),
                streamId: this.streamId,
            });
            const marketType = this.restClient.marketType ?? 'futures';
            if (marketType === 'unknown') {
                this.logErrorThrottled(`oi:${symbol}:marketType`, `[BybitREST] open interest skipped for ${symbol}: marketType=unknown`);
                this.resetOiBackoff(symbol);
                return;
            }
            const knownMarketType: KnownMarketType = marketType;
            const payload: OpenInterestEvent = {
                symbol,
                streamId: this.streamId,
                openInterest: data.openInterest,
                openInterestUnit: 'contracts',
                exchangeTs: data.ts,
                marketType: knownMarketType,
                meta,
            };
            this.bus.publish('market:oi', payload);
            this.bus.publish(
                'market:open_interest_raw',
                mapOpenInterestRaw('bybit', symbol, data.openInterest, data.ts, this.now(), undefined, this.restClient.marketType)
            );
            this.resetOiBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            const backoff = this.bumpOiBackoff(symbol);
            this.logErrorThrottled(
                `oi:${symbol}`,
                `[BybitREST] open interest failed for ${symbol}: ${(err as Error).message}`,
                err,
                backoff
            );
        } finally {
            this.oiAbortControllers.delete(symbol);
            this.oiInFlight.delete(symbol);
        }
    }

    private async pollFundingRate(symbol: string): Promise<void> {
        if (this.fundingInFlight.has(symbol)) return;
        if (!this.shouldPollFunding(symbol)) return;
        this.fundingInFlight.add(symbol);
        const controller = new AbortController();
        this.fundingAbortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchFundingRate({ symbol, signal: controller.signal });
            const meta = createMeta('market', {
                tsEvent: asTsMs(data.ts),
                tsIngest: asTsMs(this.now()),
                tsExchange: asTsMs(data.ts),
                streamId: this.streamId,
            });
            const marketType = this.restClient.marketType ?? 'futures';
            if (marketType === 'unknown') {
                this.logErrorThrottled(`funding:${symbol}:marketType`, `[BybitREST] funding skipped for ${symbol}: marketType=unknown`);
                this.resetFundingBackoff(symbol);
                return;
            }
            const knownMarketType: KnownMarketType = marketType;
            if (this.lastFundingTs.get(symbol) === data.ts) {
                this.resetFundingBackoff(symbol);
                return;
            }
            const payload: FundingRateEvent = {
                symbol,
                streamId: this.streamId,
                fundingRate: data.fundingRate,
                exchangeTs: data.ts,
                nextFundingTs: data.nextFundingTs,
                marketType: knownMarketType,
                meta,
            };
            this.bus.publish('market:funding', payload);
            this.bus.publish(
                'market:funding_raw',
                mapFundingRaw('bybit', symbol, data.fundingRate, data.ts, this.now(), data.nextFundingTs, this.restClient.marketType)
            );
            this.lastFundingTs.set(symbol, data.ts);
            this.resetFundingBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            const backoff = this.bumpFundingBackoff(symbol);
            this.logErrorThrottled(
                `funding:${symbol}`,
                `[BybitREST] funding failed for ${symbol}: ${(err as Error).message}`,
                err,
                backoff
            );
        } finally {
            this.fundingAbortControllers.delete(symbol);
            this.fundingInFlight.delete(symbol);
        }
    }

    private shouldPollOi(symbol: string): boolean {
        const state = this.oiBackoffState.get(symbol);
        if (!state) return true;
        return this.now() >= state.nextAllowedTs;
    }

    private shouldPollFunding(symbol: string): boolean {
        const state = this.fundingBackoffState.get(symbol);
        if (!state) return true;
        return this.now() >= state.nextAllowedTs;
    }

    private resetOiBackoff(symbol: string): void {
        this.oiBackoffState.delete(symbol);
    }

    private resetFundingBackoff(symbol: string): void {
        this.fundingBackoffState.delete(symbol);
    }

    private bumpOiBackoff(symbol: string): BackoffInfo {
        const existing = this.oiBackoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        const nextAllowedTs = this.now() + backoffMs;
        this.oiBackoffState.set(symbol, {
            failures,
            nextAllowedTs,
        });
        return { failures, backoffMs, nextAllowedTs };
    }

    private bumpFundingBackoff(symbol: string): BackoffInfo {
        const existing = this.fundingBackoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        const nextAllowedTs = this.now() + backoffMs;
        this.fundingBackoffState.set(symbol, {
            failures,
            nextAllowedTs,
        });
        return { failures, backoffMs, nextAllowedTs };
    }

    private computeBackoffMs(symbol: string, failures: number): number {
        const exp = Math.min(6, failures);
        const base = this.oiIntervalMs;
        const backoff = Math.min(300_000, base * Math.pow(2, exp));
        const jitter = Math.floor(backoff * 0.1 * stableJitterFactor(symbol, failures));
        return backoff + jitter;
    }

    private logErrorThrottled(key: string, message: string, err?: unknown, backoff?: BackoffInfo): void {
        const now = this.now();
        const last = this.lastErrorLogAt.get(key) ?? 0;
        if (now - last < 30_000) return;
        this.lastErrorLogAt.set(key, now);
        const details = this.formatRestError(err);
        const classification = formatRestClassification(err, backoff);
        const full = [message, details, classification].filter(Boolean).join(' ');
        logger.warn(m('warn', full));
    }

    private formatRestError(err: unknown): string | undefined {
        if (!(err instanceof BybitRestError)) return undefined;
        const details = err.details;
        if (!details || typeof details !== 'object') return undefined;

        const parts: string[] = [];
        pushDetail(parts, 'status', details.status);
        pushDetail(parts, 'statusText', details.statusText);
        pushDetail(parts, 'retCode', details.retCode);
        pushDetail(parts, 'retMsg', details.retMsg);
        pushDetail(parts, 'endpoint', details.endpoint);
        pushDetail(parts, 'params', details.params);
        pushDetail(parts, 'requestId', details.requestId);
        pushDetail(parts, 'rateLimit', details.rateLimit);
        pushDetail(parts, 'listLength', details.listLength);

        return parts.length ? `details=${parts.join(' ')}` : undefined;
    }
}

interface BackoffInfo {
    failures: number;
    backoffMs: number;
    nextAllowedTs: number;
}

function pushDetail(parts: string[], key: string, value: unknown): void {
    if (value === undefined || value === null) return;
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        parts.push(`${key}=${value}`);
        return;
    }
    parts.push(`${key}=${JSON.stringify(value)}`);
}

function formatRestClassification(err: unknown, backoff?: BackoffInfo): string | undefined {
    const classification = classifyRestError(err);
    const parts: string[] = [];
    if (classification.category) parts.push(`category=${classification.category}`);
    if (classification.status !== undefined) parts.push(`status=${classification.status}`);
    if (classification.retryAfter) parts.push(`retryAfter=${classification.retryAfter}`);
    if (classification.errorCode) parts.push(`errorCode=${classification.errorCode}`);
    if (classification.requestId) parts.push(`requestId=${classification.requestId}`);
    if (backoff) {
        parts.push(`attempt=${backoff.failures}`);
        parts.push(`backoffMs=${backoff.backoffMs}`);
        parts.push(`nextAllowedTs=${backoff.nextAllowedTs}`);
    }
    return parts.length ? `meta=${parts.join(' ')}` : undefined;
}

function classifyRestError(err: unknown): {
    category: string;
    status?: number;
    retryAfter?: string;
    errorCode?: string;
    requestId?: string;
} {
    if (isAbortError(err)) return { category: 'abort' };
    if (err instanceof BybitRestError) {
        const details = err.details as Record<string, unknown> | undefined;
        const status = typeof details?.status === 'number' ? details.status : undefined;
        const requestId = typeof details?.requestId === 'string' ? details.requestId : undefined;
        const rateLimit = details?.rateLimit as Record<string, string> | undefined;
        const retryAfter = rateLimit?.['retry-after'];
        const errorCode = typeof details?.retCode === 'number' ? String(details.retCode) : undefined;
        if (status === 418 || status === 429 || retryAfter) {
            return { category: 'rate_limit', status, retryAfter, requestId, errorCode };
        }
        if (status !== undefined) {
            if (status >= 500) return { category: 'http_5xx', status, retryAfter, requestId, errorCode };
            if (status >= 400) return { category: 'http_4xx', status, retryAfter, requestId, errorCode };
        }
        return { category: 'exchange_error', status, retryAfter, requestId, errorCode };
    }
    const error = err as { code?: string; message?: string } | undefined;
    const code = error?.code?.toString();
    if (code && ['ECONNRESET', 'ETIMEDOUT', 'ENOTFOUND', 'EAI_AGAIN', 'ECONNREFUSED'].includes(code)) {
        return { category: 'network', errorCode: code };
    }
    return { category: 'unknown' };
}
