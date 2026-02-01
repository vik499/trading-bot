import { describe, expect, it, vi } from 'vitest';
import { createMeta } from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketGateway } from '../src/exchange/marketGateway';

describe('MarketGateway Kline Bootstrap Routing', () => {
  it('ignores bootstrap for other venue', async () => {
    const bus = createTestEventBus();
    const restClient = { fetchKlines: vi.fn().mockResolvedValue({ klines: [], meta: {} }) };
    const wsClient = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      subscribeTicker: vi.fn(),
      subscribeTrades: vi.fn(),
      subscribeOrderbook: vi.fn(),
      isAlive: () => true,
    };
    const derivativesPoller = { startForSymbol: vi.fn(), stop: vi.fn() };

    const gateway = new MarketGateway(wsClient as any, restClient as any, bus, derivativesPoller as any, {
      venue: 'bybit',
      marketType: 'futures',
    });

    gateway.start();

    bus.publish('market:kline_bootstrap_requested', {
      meta: createMeta('system'),
      symbol: 'BTCUSDT',
      interval: '1',
      tf: '1m',
      limit: 1,
      venue: 'binance',
      marketType: 'futures',
    });

    await Promise.resolve();

    expect(restClient.fetchKlines).not.toHaveBeenCalled();

    gateway.stop();
  });
});
