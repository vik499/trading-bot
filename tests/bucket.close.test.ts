import { describe, expect, it } from 'vitest';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdEvent } from '../src/core/events/EventBus';

describe('bucketCloseTs', () => {
  it('assigns trades around boundary to correct bucket close', () => {
    const bus = createTestEventBus();
    const cvd = new CvdCalculator(bus, { bucketMs: 1000, marketTypes: ['futures'] });
    const outputs: MarketCvdEvent[] = [];
    bus.subscribe('market:cvd_futures', (evt) => outputs.push(evt));
    cvd.start();

    bus.publish('market:trade', {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Buy',
      price: 100,
      size: 1,
      tradeTs: 999,
      exchangeTs: 999,
      marketType: 'futures',
      meta: createMeta('market', { ts: 999 }),
    });

    bus.publish('market:trade', {
      symbol: 'BTCUSDT',
      streamId: 's1',
      side: 'Sell',
      price: 100,
      size: 1,
      tradeTs: 1000,
      exchangeTs: 1000,
      marketType: 'futures',
      meta: createMeta('market', { ts: 1000 }),
    });

    expect(outputs).toHaveLength(1);
    const first = outputs[0];
    expect(first.bucketStartTs).toBe(0);
    expect(first.bucketEndTs).toBe(1000);

    cvd.stop();
  });
});
