import { describe, expect, it } from 'vitest';
import { OkxPublicWsClient } from '../../../src/exchange/okx/wsClient';
import { EventBus } from '../../../src/core/events/EventBus';

describe('OkxPublicWsClient seq normalization', () => {
  it('uses normalized seq to avoid false gaps and triggers resync on real gaps', () => {
    const bus = new EventBus();
    const resyncs: Array<{ reason: string }> = [];

    bus.subscribe('market:resync_requested', (evt) => {
      resyncs.push({ reason: evt.reason });
    });

    let now = 1_000;
    const client = new OkxPublicWsClient('wss://ws.okx.com:8443/ws/v5/public', {
      streamId: 'okx.public.swap',
      marketType: 'futures',
      eventBus: bus,
      now: () => {
        now += 100;
        return now;
      },
      resyncCooldownMs: 0,
    });

    const snapshot = JSON.stringify({
      arg: { channel: 'books', instId: 'BTC-USDT-SWAP' },
      action: 'snapshot',
      data: [
        {
          ts: '1700000000000',
          seqId: '10',
          bids: [['100', '1']],
          asks: [['101', '1']],
        },
      ],
    });

    const updateSeq = JSON.stringify({
      arg: { channel: 'books', instId: 'BTC-USDT-SWAP' },
      action: 'update',
      data: [
        {
          ts: '1700000000100',
          seq: '11',
          bids: [['100', '2']],
          asks: [['101', '1']],
        },
      ],
    });

    const updateGap = JSON.stringify({
      arg: { channel: 'books', instId: 'BTC-USDT-SWAP' },
      action: 'update',
      data: [
        {
          ts: '1700000000200',
          seqId: '13',
          bids: [['100', '3']],
          asks: [['101', '1']],
        },
      ],
    });

    (client as any).onMessage(snapshot);
    (client as any).onMessage(updateSeq);
    (client as any).onMessage(updateGap);

    expect(resyncs).toHaveLength(1);
    expect(resyncs[0].reason).toBe('gap');
  });
});
