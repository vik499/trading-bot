import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createMeta,
  type BotEventMap,
  type EventBus,
  type Kline,
  type KlineBootstrapCompleted,
  type KlineBootstrapFailed,
  type KlineEvent,
  type KlineBootstrapRequested,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketGateway, type KlineRestClient, type MarketWsClient } from '../src/exchange/marketGateway';
import type { KlineFetchResult } from '../src/exchange/bybit/restClient';
import { logger } from '../src/infra/logger';

function waitForEvent<T extends keyof BotEventMap>(bus: EventBus, topic: T, timeoutMs = 1500): Promise<BotEventMap[T][0]> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      bus.unsubscribe(topic, handler as any);
      reject(new Error(`Timed out waiting for event ${topic}`));
    }, timeoutMs);

    const handler = (payload: BotEventMap[T][0]) => {
      clearTimeout(timer);
      bus.unsubscribe(topic, handler as any);
      resolve(payload);
    };

    bus.subscribe(topic, handler as any);
  });
}

class StubWsClient implements MarketWsClient {
  async connect(): Promise<void> {}
  async disconnect(): Promise<void> {}
  subscribeTicker(_symbol: string): void {}
  subscribeTrades(_symbol: string): void {}
  subscribeOrderbook(_symbol: string): void {}
  isAlive(): boolean {
    return true;
  }
}

class StubRestClient implements KlineRestClient {
  constructor(private readonly response: KlineFetchResult | Error) {}

  async fetchKlines(): Promise<KlineFetchResult> {
    if (this.response instanceof Error) throw this.response;
    return this.response;
  }
}

class MultiRestClient implements KlineRestClient {
  constructor(private readonly responses: Record<string, Kline[]>) {}

  async fetchKlines(params: { symbol: string; interval: string; limit?: number; sinceTs?: number }): Promise<KlineFetchResult> {
    const list = this.responses[params.interval] ?? [];
    return {
      klines: list,
      meta: {
        url: 'http://stub',
        endpoint: '/v5/market/kline',
        params: {
          symbol: params.symbol,
          interval: params.interval,
          limit: String(params.limit ?? 200),
        },
        status: 200,
        statusText: 'OK',
        listLength: list.length,
      },
    };
  }
}

describe('MarketGateway kline bootstrap', () => {
  let bus: EventBus;
  let gateway: MarketGateway;

  beforeEach(() => {
    bus = createTestEventBus();
  });

  afterEach(() => {
    gateway?.stop();
  });

  it('emits klines in chronological order with deterministic meta.ts and correlationId', async () => {
    const klines: Kline[] = [
      {
        symbol: 'BTCUSDT',
        interval: '5',
        tf: '5m',
        startTs: 1_700_000_300_000,
        endTs: 1_700_000_600_000,
        open: 100,
        high: 110,
        low: 90,
        close: 105,
        volume: 12,
      },
      {
        symbol: 'BTCUSDT',
        interval: '5',
        tf: '5m',
        startTs: 1_700_000_000_000,
        endTs: 1_700_000_300_000,
        open: 90,
        high: 100,
        low: 85,
        close: 95,
        volume: 10,
      },
    ];

    const rest = new StubRestClient({
      klines,
      meta: {
        url: 'http://stub',
        endpoint: '/v5/market/kline',
        params: { symbol: 'BTCUSDT', interval: '5', limit: '200' },
        status: 200,
        statusText: 'OK',
        listLength: klines.length,
      },
    });
    gateway = new MarketGateway(new StubWsClient(), rest, bus);
    gateway.start();

    const emitted: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => emitted.push(evt));

    const correlationId = 'boot-corr-1';
    const req: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '5',
      tf: '5m',
      limit: 200,
      meta: createMeta('system', { correlationId }),
    };

    const waitCompleted = waitForEvent(bus, 'market:kline_bootstrap_completed');
    bus.publish('market:kline_bootstrap_requested', req);
    const completed = (await waitCompleted) as KlineBootstrapCompleted;

    expect(emitted.map((e) => e.endTs)).toEqual([1_700_000_300_000, 1_700_000_600_000]);
    emitted.forEach((evt) => {
      expect(evt.meta.ts).toBe(evt.endTs);
      expect(evt.meta.correlationId).toBe(correlationId);
    });
    expect(completed.meta.correlationId).toBe(correlationId);
    expect(completed.tf).toBe('5m');
    expect(completed.received).toBe(2);
    expect(completed.emitted).toBe(2);
    expect(completed.startTs).toBe(1_700_000_000_000);
    expect(completed.endTs).toBe(1_700_000_600_000);
  });

  it('emits bootstrap_failed on REST errors', async () => {
    const rest = new StubRestClient(new Error('REST down'));
    gateway = new MarketGateway(new StubWsClient(), rest, bus);
    gateway.start();

    const correlationId = 'boot-corr-2';
    const req: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '5',
      tf: '5m',
      limit: 200,
      meta: createMeta('system', { correlationId }),
    };

    const waitFailed = waitForEvent(bus, 'market:kline_bootstrap_failed');
    bus.publish('market:kline_bootstrap_requested', req);
    const failed = (await waitFailed) as KlineBootstrapFailed;

    expect(failed.meta.correlationId).toBe(correlationId);
    expect(failed.reason).toContain('REST down');
    expect(failed.symbol).toBe('BTCUSDT');
    expect(failed.tf).toBe('5m');
  });

  it('emits klines for each requested tf with shared correlationId', async () => {
    const rest = new MultiRestClient({
      '60': [
        {
          symbol: 'BTCUSDT',
          interval: '60',
          tf: '1h',
          startTs: 0,
          endTs: 3_600_000,
          open: 1,
          high: 1,
          low: 1,
          close: 1,
          volume: 1,
        },
      ],
      '240': [
        {
          symbol: 'BTCUSDT',
          interval: '240',
          tf: '4h',
          startTs: 0,
          endTs: 14_400_000,
          open: 1,
          high: 1,
          low: 1,
          close: 1,
          volume: 1,
        },
      ],
    });
    gateway = new MarketGateway(new StubWsClient(), rest, bus);
    gateway.start();

    const emitted: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => emitted.push(evt));

    const correlationId = 'boot-multi';

    const req1: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '60',
      tf: '1h',
      limit: 200,
      meta: createMeta('system', { correlationId }),
    };
    const req2: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '240',
      tf: '4h',
      limit: 200,
      meta: createMeta('system', { correlationId }),
    };

    const wait1 = waitForEvent(bus, 'market:kline_bootstrap_completed');
    bus.publish('market:kline_bootstrap_requested', req1);
    const completed1 = (await wait1) as KlineBootstrapCompleted;

    const wait2 = waitForEvent(bus, 'market:kline_bootstrap_completed');
    bus.publish('market:kline_bootstrap_requested', req2);
    const completed2 = (await wait2) as KlineBootstrapCompleted;

    expect(completed1.tf).toBe('1h');
    expect(completed2.tf).toBe('4h');
    expect(completed1.meta.correlationId).toBe(correlationId);
    expect(completed2.meta.correlationId).toBe(correlationId);
    expect(emitted.map((e) => e.tf)).toEqual(['1h', '4h']);
  });

  it('emits 1d klines when REST returns daily data', async () => {
    const dayMs = 86_400_000;
    const klines: Kline[] = Array.from({ length: 200 }).map((_, idx) => {
      const startTs = idx * dayMs;
      return {
        symbol: 'BTCUSDT',
        interval: '1440',
        tf: '1d',
        startTs,
        endTs: startTs + dayMs,
        open: 100,
        high: 101,
        low: 99,
        close: 100,
        volume: 10,
      };
    });

    const rest = new StubRestClient({
      klines,
      meta: {
        url: 'http://stub',
        endpoint: '/v5/market/kline',
        params: { symbol: 'BTCUSDT', interval: '1440', limit: '200' },
        status: 200,
        statusText: 'OK',
        listLength: klines.length,
      },
    });

    gateway = new MarketGateway(new StubWsClient(), rest, bus);
    gateway.start();

    const emitted: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => emitted.push(evt));

    const req: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '1440',
      tf: '1d',
      limit: 200,
      meta: createMeta('system', { correlationId: 'boot-1d' }),
    };

    const waitCompleted = waitForEvent(bus, 'market:kline_bootstrap_completed');
    bus.publish('market:kline_bootstrap_requested', req);
    const completed = (await waitCompleted) as KlineBootstrapCompleted;

    expect(emitted).toHaveLength(200);
    emitted.forEach((evt) => expect(evt.meta.ts).toBe(evt.endTs));
    expect(completed.received).toBe(200);
    expect(completed.emitted).toBe(200);
    expect(completed.tf).toBe('1d');
  });

  it('emits bootstrap_failed and logs diagnostics when REST returns empty list', async () => {
    const rest = new StubRestClient({
      klines: [],
      meta: {
        url: 'http://stub',
        endpoint: '/v5/market/kline',
        params: { symbol: 'BTCUSDT', interval: '1440', limit: '200' },
        status: 200,
        statusText: 'OK',
        retCode: 0,
        retMsg: 'OK',
        listLength: 0,
      },
    });
    gateway = new MarketGateway(new StubWsClient(), rest, bus);
    gateway.start();

    const warnSpy = vi.spyOn(logger, 'warn');

    const req: KlineBootstrapRequested = {
      symbol: 'BTCUSDT',
      interval: '1440',
      tf: '1d',
      limit: 200,
      meta: createMeta('system', { correlationId: 'boot-empty' }),
    };

    const waitFailed = waitForEvent(bus, 'market:kline_bootstrap_failed');
    bus.publish('market:kline_bootstrap_requested', req);
    const failed = (await waitFailed) as KlineBootstrapFailed;

    const logged = warnSpy.mock.calls.map((call) => call[0]).join(' ');
    expect(logged).toContain('[KlineBootstrap] empty response');
    expect(logged).toContain('interval=1440');
    expect(logged).toContain('limit=200');
    expect(logged).toContain('status=200');

    expect(failed.reason).toContain('empty list');
    expect(failed.tf).toBe('1d');

    warnSpy.mockRestore();
  });
});
