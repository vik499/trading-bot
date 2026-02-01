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
    type OpenInterestUnit,
} from '../../core/events/EventBus';
import { normalizeSymbol } from '../../core/market/symbols';
import type { OpenInterestCollector } from '../../core/market/OpenInterestCollector';
import { mapFundingRaw, mapOpenInterestRaw } from '../normalizers/rawAdapters';

export interface OkxFetchOpenInterestParams {
    instId: string;
    instType?: string;
}

export interface OkxFetchFundingRateParams {
    instId: string;
}

export interface OkxFetchKlinesParams {
    instId: string;
    bar: string;
    limit?: number;
    after?: number;
    before?: number;
}

export interface OkxRequestMeta {
    url: string;
    endpoint: string;
    params: Record<string, string>;
    status: number;
    statusText: string;
    code?: string;
    msg?: string;
    requestId?: string;
    rateLimit?: Record<string, string>;
}

export class OkxRestError extends Error {
    readonly details?: Record<string, unknown>;
    readonly originalError?: unknown;

    constructor(message: string, details?: Record<string, unknown>, originalError?: unknown) {
        super(message);
        this.name = 'OkxRestError';
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

interface OkxOpenInterestSnapshot {
    openInterest: number;
    openInterestUnit: OpenInterestUnit;
    openInterestValueUsd?: number;
    ts: number;
}

interface OkxFundingSnapshot {
    fundingRate: number;
    fundingTime?: number;
    nextFundingTime?: number;
    ts: number;
}

const DEFAULT_BASE_URL = 'https://www.okx.com';
const DEFAULT_STREAM_ID = 'okx.public.swap';
const DEFAULT_INST_TYPE = 'SWAP';

export class OkxPublicRestClient {
    public readonly streamId = DEFAULT_STREAM_ID;
    public readonly marketType: MarketType = 'futures';

    constructor(private readonly baseUrl = DEFAULT_BASE_URL) {}

    async fetchOpenInterest(params: OkxFetchOpenInterestParams & { signal?: AbortSignal }): Promise<OkxOpenInterestSnapshot> {
        const url = this.buildOpenInterestUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new OkxRestError('OKX REST open interest failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new OkxRestError('OKX REST open interest response is not an object', { ...responseMeta });
        }

        const code = payload.code !== undefined ? String(payload.code) : undefined;
        const msg = typeof payload.msg === 'string' ? payload.msg : undefined;
        if (code && code !== '0') {
            throw new OkxRestError('OKX REST open interest returned error', { ...responseMeta, code, msg });
        }

        const rows = payload.data;
        if (!Array.isArray(rows) || rows.length === 0) {
            throw new OkxRestError('OKX REST open interest response is empty', { ...responseMeta, code, msg });
        }

        const row = rows[0];
        if (!isObject(row)) {
            throw new OkxRestError('OKX REST open interest row is not an object', { ...responseMeta, code, msg });
        }

        const oi = toOptionalNumber(row.oi);
        const oiCcy = toOptionalNumber(row.oiCcy);
        const oiUsd = toOptionalNumber(row.oiUsd);
        const ts = toNumber(row.ts ?? payload.ts, 'ts');

        const { openInterest, openInterestUnit } = selectOpenInterest(oi, oiCcy, oiUsd);
        return {
            openInterest,
            openInterestUnit,
            openInterestValueUsd: oiUsd,
            ts,
        };
    }

    async fetchFundingRate(params: OkxFetchFundingRateParams & { signal?: AbortSignal }): Promise<OkxFundingSnapshot> {
        const url = this.buildFundingRateUrl(params);
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new OkxRestError('OKX REST funding rate failed', {
                ...responseMeta,
            });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new OkxRestError('OKX REST funding rate response is not an object', { ...responseMeta });
        }

        const code = payload.code !== undefined ? String(payload.code) : undefined;
        const msg = typeof payload.msg === 'string' ? payload.msg : undefined;
        if (code && code !== '0') {
            throw new OkxRestError('OKX REST funding rate returned error', { ...responseMeta, code, msg });
        }

        const rows = payload.data;
        if (!Array.isArray(rows) || rows.length === 0) {
            throw new OkxRestError('OKX REST funding rate response is empty', { ...responseMeta, code, msg });
        }

        const row = rows[0];
        if (!isObject(row)) {
            throw new OkxRestError('OKX REST funding rate row is not an object', { ...responseMeta, code, msg });
        }

        const fundingRate = toNumber(row.fundingRate, 'fundingRate');
        const fundingTime = toOptionalNumber(row.fundingTime);
        const nextFundingTime = toOptionalNumber(row.nextFundingTime);
        const ts = toNumber(row.ts ?? fundingTime ?? nextFundingTime, 'ts');

        return { fundingRate, fundingTime, nextFundingTime, ts };
    }

    async fetchKlines(params: { symbol: string; interval: KlineInterval; limit?: number; sinceTs?: number; signal?: AbortSignal }): Promise<{ klines: Kline[]; meta: OkxRequestMeta }> {
        const instId = toOkxSwapInstId(params.symbol);
        if (!instId) {
            throw new OkxRestError('OKX REST kline invalid symbol', { symbol: params.symbol });
        }
        const bar = toOkxBar(params.interval);
        if (!bar) {
            throw new OkxRestError('OKX REST kline unsupported interval', { interval: params.interval });
        }

        const url = this.buildKlinesUrl({
            instId,
            bar,
            limit: params.limit ?? 200,
            after: params.sinceTs,
        });
        const result = await fetchJson(url, params.signal);
        const requestMeta = buildRequestMeta(url);
        const responseMeta = buildResponseMeta(result, requestMeta);

        if (!result.ok) {
            throw new OkxRestError('OKX REST kline failed', { ...responseMeta });
        }

        const payload = result.data;
        if (!isObject(payload)) {
            throw new OkxRestError('OKX REST kline response is not an object', { ...responseMeta });
        }

        const code = payload.code !== undefined ? String(payload.code) : undefined;
        const msg = typeof payload.msg === 'string' ? payload.msg : undefined;
        if (code && code !== '0') {
            throw new OkxRestError('OKX REST kline returned error', { ...responseMeta, code, msg });
        }

        const rows = payload.data;
        if (!Array.isArray(rows)) {
            throw new OkxRestError('OKX REST kline response is not an array', { ...responseMeta, code, msg });
        }

        const marketType = this.marketType ?? 'futures';
        if (marketType === 'unknown') {
            logger.warn(m('warn', `[OKXREST] klines skipped for ${params.symbol}: marketType=unknown`));
            return {
                klines: [],
                meta: { ...responseMeta, code, msg },
            };
        }
        const knownMarketType: KnownMarketType = marketType;
        const klines = rows.map((row) => parseKlineRow(row, params.symbol, params.interval, this.streamId, knownMarketType));
        return {
            klines,
            meta: { ...responseMeta, code, msg },
        };
    }

    private buildOpenInterestUrl(params: OkxFetchOpenInterestParams): string {
        const url = new URL('/api/v5/public/open-interest', this.baseUrl);
        url.searchParams.set('instType', params.instType ?? DEFAULT_INST_TYPE);
        url.searchParams.set('instId', params.instId);
        return url.toString();
    }

    private buildFundingRateUrl(params: OkxFetchFundingRateParams): string {
        const url = new URL('/api/v5/public/funding-rate', this.baseUrl);
        url.searchParams.set('instId', params.instId);
        return url.toString();
    }

    private buildKlinesUrl(params: OkxFetchKlinesParams): string {
        const url = new URL('/api/v5/market/candles', this.baseUrl);
        url.searchParams.set('instId', params.instId);
        url.searchParams.set('bar', params.bar);
        if (params.limit !== undefined) url.searchParams.set('limit', String(params.limit));
        if (params.after !== undefined) url.searchParams.set('after', String(params.after));
        if (params.before !== undefined) url.searchParams.set('before', String(params.before));
        return url.toString();
    }
}

export interface OkxOpenInterestCollectorOptions {
    oiIntervalMs?: number;
    streamId?: string;
    now?: () => number;
}

export class OkxOpenInterestCollector implements OpenInterestCollector {
    private readonly restClient: OkxPublicRestClient;
    private readonly bus: EventBus;
    private readonly oiIntervalMs: number;
    private readonly streamId: string;
    private readonly now: () => number;
    private readonly timers = new Map<string, NodeJS.Timeout>();
    private readonly inFlight = new Set<string>();
    private readonly lastEmittedTs = new Map<string, number>();
    private readonly lastErrorLogAt = new Map<string, number>();
    private readonly backoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly instIdBySymbol = new Map<string, string>();
    private readonly abortControllers = new Map<string, AbortController>();

    constructor(
        restClient: OkxPublicRestClient = new OkxPublicRestClient(),
        bus: EventBus = eventBus,
        opts: OkxOpenInterestCollectorOptions = {}
    ) {
        this.restClient = restClient;
        this.bus = bus;
        this.oiIntervalMs = Math.max(10_000, opts.oiIntervalMs ?? 30_000);
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.now = opts.now ?? nowMs;
    }

    startForSymbol(symbol: string): void {
        const key = normalizeSymbol(symbol);
        const instId = toOkxSwapInstId(key);
        if (!instId) return;
        this.instIdBySymbol.set(key, instId);
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
        this.instIdBySymbol.clear();
    }

    private async pollOpenInterest(symbol: string): Promise<void> {
        const instId = this.instIdBySymbol.get(symbol) ?? toOkxSwapInstId(symbol);
        if (!instId) return;
        if (this.inFlight.has(symbol)) return;
        if (!this.shouldPoll(symbol)) return;
        this.inFlight.add(symbol);
        const controller = new AbortController();
        this.abortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchOpenInterest({ instId, signal: controller.signal });
            const meta = createMeta('okx', {
                tsEvent: asTsMs(data.ts),
                tsIngest: asTsMs(this.now()),
                tsExchange: asTsMs(data.ts),
                streamId: this.streamId,
            });
            const marketType = this.restClient.marketType ?? 'futures';
            if (marketType === 'unknown') {
                this.logErrorThrottled(`oi:${instId}:marketType`, `[OKXREST] open interest skipped for ${instId}: marketType=unknown`);
                this.resetBackoff(symbol);
                return;
            }
            const knownMarketType: KnownMarketType = marketType;

            if (this.lastEmittedTs.get(symbol) === data.ts) {
                this.resetBackoff(symbol);
                return;
            }

            const payload: OpenInterestEvent = {
                symbol,
                streamId: this.streamId,
                openInterest: data.openInterest,
                openInterestUnit: data.openInterestUnit,
                openInterestValueUsd: data.openInterestValueUsd,
                exchangeTs: data.ts,
                marketType: knownMarketType,
                meta,
            };
            this.bus.publish('market:oi', payload);
            this.bus.publish(
                'market:open_interest_raw',
                mapOpenInterestRaw('okx', instId, data.openInterest, data.ts, this.now(), data.openInterestValueUsd, marketType)
            );
            this.lastEmittedTs.set(symbol, data.ts);
            this.resetBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            const backoff = this.bumpBackoff(symbol);
            this.logErrorThrottled(
                `oi:${instId}`,
                `[OkxREST] open interest failed for ${instId}: ${(err as Error).message}`,
                err,
                backoff
            );
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

    private bumpBackoff(symbol: string): BackoffInfo {
        const existing = this.backoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        const nextAllowedTs = this.now() + backoffMs;
        this.backoffState.set(symbol, {
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
        const details = formatRestError(err);
        const classification = formatRestClassification(err, backoff);
        const full = [message, details, classification].filter(Boolean).join(' ');
        logger.warn(m('warn', full));
    }
}

export interface OkxFundingCollectorOptions {
    fundingIntervalMs?: number;
    streamId?: string;
    now?: () => number;
}

export class OkxFundingCollector implements OpenInterestCollector {
    private readonly restClient: OkxPublicRestClient;
    private readonly bus: EventBus;
    private readonly fundingIntervalMs: number;
    private readonly streamId: string;
    private readonly now: () => number;
    private readonly timers = new Map<string, NodeJS.Timeout>();
    private readonly inFlight = new Set<string>();
    private readonly lastEmittedTs = new Map<string, number>();
    private readonly lastErrorLogAt = new Map<string, number>();
    private readonly backoffState = new Map<string, { failures: number; nextAllowedTs: number }>();
    private readonly instIdBySymbol = new Map<string, string>();
    private readonly abortControllers = new Map<string, AbortController>();

    constructor(
        restClient: OkxPublicRestClient = new OkxPublicRestClient(),
        bus: EventBus = eventBus,
        opts: OkxFundingCollectorOptions = {}
    ) {
        this.restClient = restClient;
        this.bus = bus;
        this.fundingIntervalMs = Math.max(10_000, opts.fundingIntervalMs ?? 60_000);
        this.streamId = opts.streamId ?? DEFAULT_STREAM_ID;
        this.now = opts.now ?? nowMs;
    }

    startForSymbol(symbol: string): void {
        const key = normalizeSymbol(symbol);
        const instId = toOkxSwapInstId(key);
        if (!instId) return;
        this.instIdBySymbol.set(key, instId);
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
        this.instIdBySymbol.clear();
    }

    private async pollFunding(symbol: string): Promise<void> {
        const instId = this.instIdBySymbol.get(symbol) ?? toOkxSwapInstId(symbol);
        if (!instId) return;
        if (this.inFlight.has(symbol)) return;
        if (!this.shouldPoll(symbol)) return;
        this.inFlight.add(symbol);
        const controller = new AbortController();
        this.abortControllers.set(symbol, controller);
        try {
            const data = await this.restClient.fetchFundingRate({ instId, signal: controller.signal });
            const meta = createMeta('okx', {
                tsEvent: asTsMs(data.ts),
                tsIngest: asTsMs(this.now()),
                tsExchange: asTsMs(data.fundingTime ?? data.ts),
                streamId: this.streamId,
            });
            const marketType = this.restClient.marketType ?? 'futures';
            if (marketType === 'unknown') {
                this.logErrorThrottled(`funding:${instId}:marketType`, `[OKXREST] funding skipped for ${instId}: marketType=unknown`);
                this.resetBackoff(symbol);
                return;
            }
            const knownMarketType: KnownMarketType = marketType;

            if (this.lastEmittedTs.get(symbol) === data.ts) {
                this.resetBackoff(symbol);
                return;
            }

            const payload: FundingRateEvent = {
                symbol,
                streamId: this.streamId,
                fundingRate: data.fundingRate,
                exchangeTs: data.fundingTime ?? data.ts,
                nextFundingTs: data.nextFundingTime,
                marketType: knownMarketType,
                meta,
            };
            this.bus.publish('market:funding', payload);
            this.bus.publish(
                'market:funding_raw',
                mapFundingRaw('okx', instId, data.fundingRate, data.fundingTime ?? data.ts, this.now(), data.nextFundingTime, marketType)
            );
            this.lastEmittedTs.set(symbol, data.ts);
            this.resetBackoff(symbol);
        } catch (err) {
            if (isAbortError(err)) return;
            const backoff = this.bumpBackoff(symbol);
            this.logErrorThrottled(
                `funding:${instId}`,
                `[OkxREST] funding failed for ${instId}: ${(err as Error).message}`,
                err,
                backoff
            );
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

    private bumpBackoff(symbol: string): BackoffInfo {
        const existing = this.backoffState.get(symbol) ?? { failures: 0, nextAllowedTs: 0 };
        const failures = existing.failures + 1;
        const backoffMs = this.computeBackoffMs(symbol, failures);
        const nextAllowedTs = this.now() + backoffMs;
        this.backoffState.set(symbol, {
            failures,
            nextAllowedTs,
        });
        return { failures, backoffMs, nextAllowedTs };
    }

    private computeBackoffMs(symbol: string, failures: number): number {
        const exp = Math.min(6, failures);
        const base = this.fundingIntervalMs;
        const backoff = Math.min(300_000, base * Math.pow(2, exp));
        const jitter = Math.floor(backoff * 0.1 * stableJitterFactor(symbol, failures));
        return backoff + jitter;
    }

    private logErrorThrottled(key: string, message: string, err?: unknown, backoff?: BackoffInfo): void {
        const now = this.now();
        const last = this.lastErrorLogAt.get(key) ?? 0;
        if (now - last < 30_000) return;
        this.lastErrorLogAt.set(key, now);
        const details = formatRestError(err);
        const classification = formatRestClassification(err, backoff);
        const full = [message, details, classification].filter(Boolean).join(' ');
        logger.warn(m('warn', full));
    }
}

interface BackoffInfo {
    failures: number;
    backoffMs: number;
    nextAllowedTs: number;
}

export interface OkxDerivativesPollerOptions {
    oiIntervalMs?: number;
    fundingIntervalMs?: number;
    streamId?: string;
    now?: () => number;
}

export class OkxDerivativesRestPoller implements OpenInterestCollector {
    private readonly oiCollector: OkxOpenInterestCollector;
    private readonly fundingCollector: OkxFundingCollector;

    constructor(
        restClient: OkxPublicRestClient = new OkxPublicRestClient(),
        bus: EventBus = eventBus,
        opts: OkxDerivativesPollerOptions = {}
    ) {
        this.oiCollector = new OkxOpenInterestCollector(restClient, bus, {
            oiIntervalMs: opts.oiIntervalMs,
            streamId: opts.streamId,
            now: opts.now,
        });
        this.fundingCollector = new OkxFundingCollector(restClient, bus, {
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
            reject(new OkxRestError('OKX REST aborted', { url }, new Error('AbortError')));
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
                    reject(new OkxRestError('OKX REST invalid JSON', { status, statusText, url }, error));
                }
            });
        });

        request.on('error', (error) => {
            reject(new OkxRestError('OKX REST request error', { url }, error));
        });

        if (signal) {
            const onAbort = () => {
                request.destroy(new Error('AbortError'));
                reject(new OkxRestError('OKX REST aborted', { url }, new Error('AbortError')));
            };
            signal.addEventListener('abort', onAbort, { once: true });
            request.on('close', () => signal.removeEventListener('abort', onAbort));
        }
    });
}

function isAbortError(err: unknown): boolean {
    if (err instanceof OkxRestError) {
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
        throw new OkxRestError('OKX REST invalid number', { label, value });
    }
    return num;
}

function toOptionalNumber(value: unknown): number | undefined {
    if (value === undefined || value === null || value === '') return undefined;
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : undefined;
}

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function toOkxSwapInstId(symbol: string): string | undefined {
    const raw = symbol.trim().toUpperCase();
    if (!raw) return undefined;
    if (raw.endsWith('-SWAP')) return raw;
    if (raw.includes('-')) {
        const parts = raw.split('-').filter(Boolean);
        if (parts.length === 2) {
            return `${parts[0]}-${parts[1]}-SWAP`;
        }
    }
    const match = raw.match(/^([A-Z0-9]+)(USDT|USDC|USD)$/);
    if (!match) return undefined;
    return `${match[1]}-${match[2]}-SWAP`;
}

function toOkxBar(interval: KlineInterval): string | undefined {
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
            return '1H';
        case '120':
            return '2H';
        case '240':
            return '4H';
        case '360':
            return '6H';
        case '720':
            return '12H';
        case '1440':
            return '1D';
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

function intervalToMs(interval: KlineInterval): number {
    const minutes = Number.parseInt(interval, 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return 0;
    return minutes * 60_000;
}

function parseKlineRow(row: unknown, symbol: string, interval: KlineInterval, streamId: string, marketType: KnownMarketType): Kline {
    if (!Array.isArray(row) || row.length < 6) {
        throw new OkxRestError('OKX REST kline row invalid', { rowPreview: JSON.stringify(row) });
    }
    const startTs = toNumber(row[0], 'ts');
    const open = toNumber(row[1], 'open');
    const high = toNumber(row[2], 'high');
    const low = toNumber(row[3], 'low');
    const close = toNumber(row[4], 'close');
    const volume = toNumber(row[5], 'volume');
    const endTs = startTs + intervalToMs(interval);
    return {
        symbol,
        streamId,
        marketType,
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

function selectOpenInterest(
    oi: number | undefined,
    oiCcy: number | undefined,
    oiUsd: number | undefined
): { openInterest: number; openInterestUnit: OpenInterestUnit } {
    if (oiCcy !== undefined) {
        return { openInterest: oiCcy, openInterestUnit: 'base' };
    }
    if (oi !== undefined) {
        return { openInterest: oi, openInterestUnit: 'contracts' };
    }
    if (oiUsd !== undefined) {
        return { openInterest: oiUsd, openInterestUnit: 'usd' };
    }
    throw new OkxRestError('OKX REST open interest missing values');
}

function stableJitterFactor(key: string, failures: number): number {
    const input = `${key}:${failures}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
    }
    return (hash % 1000) / 1000;
}

function buildRequestMeta(url: string): Pick<OkxRequestMeta, 'url' | 'endpoint' | 'params'> {
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

function buildResponseMeta(
    result: HttpJsonResult,
    request: Pick<OkxRequestMeta, 'url' | 'endpoint' | 'params'>
): OkxRequestMeta {
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
    const candidates = ['x-okx-request-id', 'okx-request-id', 'x-okx-trace-id', 'okx-trace-id', 'x-request-id'];
    for (const key of candidates) {
        const value = headers[key];
        if (value) return value;
    }
    return undefined;
}

function extractRateLimit(headers: Record<string, string>): Record<string, string> | undefined {
    const keys = [
        'x-ratelimit-limit',
        'x-ratelimit-remaining',
        'x-ratelimit-reset',
        'x-ratelimit-remaining-1s',
        'x-ratelimit-remaining-2s',
        'x-ratelimit-remaining-10s',
        'x-ratelimit-remaining-1m',
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
    if (!(err instanceof OkxRestError)) return undefined;
    const details = err.details;
    if (!details || typeof details !== 'object') return undefined;

    const parts: string[] = [];
    pushDetail(parts, 'status', details.status);
    pushDetail(parts, 'statusText', details.statusText);
    pushDetail(parts, 'code', details.code);
    pushDetail(parts, 'msg', details.msg);
    pushDetail(parts, 'endpoint', details.endpoint);
    pushDetail(parts, 'params', details.params);
    pushDetail(parts, 'requestId', details.requestId);
    pushDetail(parts, 'rateLimit', details.rateLimit);

    return parts.length ? `details=${parts.join(' ')}` : undefined;
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
    if (err instanceof OkxRestError) {
        const details = err.details as Record<string, unknown> | undefined;
        const status = typeof details?.status === 'number' ? details.status : undefined;
        const requestId = typeof details?.requestId === 'string' ? details.requestId : undefined;
        const rateLimit = details?.rateLimit as Record<string, string> | undefined;
        const retryAfter = rateLimit?.['retry-after'];
        const errorCode = typeof details?.code === 'string' ? details?.code : undefined;
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

function pushDetail(parts: string[], key: string, value: unknown): void {
    if (value === undefined || value === null) return;
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        parts.push(`${key}=${value}`);
        return;
    }
    parts.push(`${key}=${JSON.stringify(value)}`);
}
