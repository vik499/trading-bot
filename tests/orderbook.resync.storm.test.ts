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

describe('MarketGateway resync storm protection', () => {
  it('coalesces repeated resync requests', async () => {
    const bus = createTestEventBus();
    const ws = new MockWsClient();
    const gateway = new MarketGateway(ws, undefined, bus, undefined, { resyncCooldownMs: 1_000, venue: 'binance' });
    gateway.start();

    const meta = createMeta('market', { tsEvent: 1_000, tsIngest: 1_000, streamId: 'binance.usdm.public' });
    for (let i = 0; i < 5; i += 1) {
      const evt: MarketResyncRequested = {
        venue: 'binance',
        symbol: 'BTCUSDT',
        channel: 'orderbook',
        reason: 'gap',
        meta,
      };
      bus.publish('market:resync_requested', evt);
    }

    await Promise.resolve();
    await Promise.resolve();

    expect(ws.disconnectCount).toBe(1);
    expect(ws.connectCount).toBe(1);

    gateway.stop();
  });
});
