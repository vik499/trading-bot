import { describe, expect, it } from 'vitest';
import { createTestEventBus } from '../src/core/events/testing';
import { BinancePublicWsClient } from '../src/exchange/binance/wsClient';
import type { OrderbookL2DeltaEvent } from '../src/core/events/EventBus';

const makeDepth = (uStart: number, uEnd: number, ts: number) => ({
  s: 'BTCUSDT',
  U: uStart,
  u: uEnd,
  E: ts,
  b: [['100', '1']],
  a: [['101', '1']],
});

describe('Orderbook delta ordering', () => {
  it('ignores duplicate and out-of-order deltas deterministically', async () => {
    const bus = createTestEventBus();
    const deltas: OrderbookL2DeltaEvent[] = [];
    const resyncs: any[] = [];

    bus.subscribe('market:orderbook_l2_delta', (evt) => deltas.push(evt));
    bus.subscribe('market:resync_requested', (evt) => resyncs.push(evt));

    const restClient = {
      fetchDepthSnapshot: async () => ({ lastUpdateId: 10, bids: [], asks: [] }),
    } as any;

    const client = new BinancePublicWsClient('wss://test', {
      eventBus: bus,
      restClient,
    });

    await (client as any).ensureDepthSnapshot('BTCUSDT');

    (client as any).handleDepth(makeDepth(11, 12, 1000));
    (client as any).handleDepth(makeDepth(11, 12, 1001)); // duplicate
    (client as any).handleDepth(makeDepth(9, 10, 1002)); // out-of-order

    expect(deltas).toHaveLength(1);
    expect(resyncs).toHaveLength(0);
  });
});
