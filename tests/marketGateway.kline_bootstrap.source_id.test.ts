import { describe, expect, it, vi } from 'vitest';
import { createMeta, type Kline } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketGateway } from '../src/exchange/marketGateway';

describe('MarketGateway kline bootstrap sourceId normalization', () => {
  it('normalizes .kline streamId for bootstrap events', async () => {
    const bus = createTestEventBus();
    const wsClient = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      subscribeTicker: vi.fn(),
      subscribeTrades: vi.fn(),
      subscribeOrderbook: vi.fn(),
      isAlive: () => true,
    };
    const derivativesPoller = { startForSymbol: vi.fn(), stop: vi.fn() };
    const restClient = {
      streamId: 'okx.public.swap.kline',
      marketType: 'futures',
      fetchKlines: vi.fn().mockResolvedValue({
        klines: [
          {
            symbol: 'BTCUSDT',
            interval: '1',
            tf: '1m',
            startTs: 0,
            endTs: 60_000,
            open: 1,
            high: 1,
            low: 1,
            close: 1,
            volume: 1,
          } as Kline,
        ],
        meta: { status: 200, statusText: 'ok', url: 'n/a', endpoint: '/kline', params: {} },
      }),
    };

    const outputs: Array<{ streamId?: string; metaSource?: string }> = [];
    bus.subscribe('market:kline', (evt) => outputs.push({ streamId: evt.streamId, metaSource: evt.meta.source }));

    const gateway = new MarketGateway(wsClient as any, restClient as any, bus, derivativesPoller as any, {
      venue: 'okx',
      marketType: 'futures',
    });

    gateway.start();

    bus.publish('market:kline_bootstrap_requested', {
      meta: createMeta('system'),
      symbol: 'BTCUSDT',
      interval: '1',
      tf: '1m',
      limit: 1,
      venue: 'okx',
      marketType: 'futures',
    });

    await Promise.resolve();

    expect(outputs).toHaveLength(1);
    expect(outputs[0].streamId).toBe('okx.public.swap');
    expect(outputs[0].metaSource).toBe('okx.public.swap');

    gateway.stop();
  });
});
