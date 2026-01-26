import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { MarketGateway, type MarketWsClient } from '../src/exchange/marketGateway';
import { createMeta, type MarketResyncRequested } from '../src/core/events/EventBus';

class MockWsClient implements MarketWsClient {
  connectCount = 0;
  disconnectCount = 0;
  alive = false;
  subscribeTicker(): void {}
  subscribeTrades(): void {}
  subscribeOrderbook(): void {}
  subscribeKlines(): void {}
  subscribeLiquidations(): void {}
  isAlive(): boolean {
    return this.alive;
  }
  async connect(): Promise<void> {
    this.connectCount += 1;
    this.alive = true;
  }
  async disconnect(): Promise<void> {
    this.disconnectCount += 1;
    this.alive = false;
  }
}

describe('MarketGateway resync cooldown', () => {
  it('throttles repeated resync requests per reason', async () => {
    const bus = createTestEventBus();
    const ws = new MockWsClient();
    const gateway = new MarketGateway(ws, undefined, bus, undefined, {
      venue: 'binance',
      marketType: 'futures',
      resyncCooldownMs: 0,
      resyncReasonCooldownMs: 2000,
      resyncLogCooldownMs: 0,
    });
    gateway.start();

    const first: MarketResyncRequested = {
      venue: 'binance',
      symbol: 'BTCUSDT',
      channel: 'orderbook',
      reason: 'gap',
      meta: createMeta('market', { ts: 1000 }),
    };
    const second: MarketResyncRequested = {
      ...first,
      meta: createMeta('market', { ts: 1500 }),
    };

    bus.publish('market:resync_requested', first);
    bus.publish('market:resync_requested', second);

    await Promise.resolve();
    await Promise.resolve();

    expect(ws.disconnectCount).toBe(1);
    expect(ws.connectCount).toBe(1);

    gateway.stop();
  });
});
