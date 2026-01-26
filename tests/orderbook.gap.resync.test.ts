import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { BinancePublicWsClient } from '../src/exchange/binance/wsClient';
import { OkxPublicWsClient } from '../src/exchange/okx/wsClient';
import type { MarketResyncRequested } from '../src/core/events/EventBus';

const wait = (ms = 5) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Orderbook gap detection -> resync', () => {
  it('emits resync for Binance depth gap', async () => {
    const bus = createTestEventBus();
    const resync: MarketResyncRequested[] = [];
    bus.subscribe('market:resync_requested', (evt) => resync.push(evt));

    const restClient = {
      fetchDepthSnapshot: async () => ({ lastUpdateId: 10, bids: [], asks: [] }),
    } as any;

    const client = new BinancePublicWsClient('wss://test', {
      eventBus: bus,
      restClient,
      supportsOrderbook: true,
    });

    await (client as any).ensureDepthSnapshot('BTCUSDT');

    (client as any).handleDepth({
      s: 'BTCUSDT',
      U: 12,
      u: 13,
      E: 1000,
      b: [],
      a: [],
    });

    await wait();
    expect(resync).toHaveLength(1);
    expect(resync[0].reason).toBe('gap');
  });

  it('emits resync for OKX book gap', () => {
    const bus = createTestEventBus();
    const resync: MarketResyncRequested[] = [];
    bus.subscribe('market:resync_requested', (evt) => resync.push(evt));

    const client = new OkxPublicWsClient('wss://test', { eventBus: bus });

    (client as any).handleOrderbook('BTC-USDT-SWAP', 'snapshot', [
      { ts: '1000', bids: [], asks: [], seqId: '10' },
    ]);

    (client as any).handleOrderbook('BTC-USDT-SWAP', 'update', [
      { ts: '1100', bids: [], asks: [], seqId: '12' },
    ]);

    expect(resync).toHaveLength(1);
    expect(resync[0].reason).toBe('gap');
  });
});
