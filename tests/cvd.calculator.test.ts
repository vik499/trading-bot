import { describe, expect, it } from 'vitest';
import { CvdCalculator } from '../src/globalData/CvdCalculator';
import { createTestEventBus } from '../src/core/events/testing';
import { createMeta, type MarketCvdEvent, type TradeEvent } from '../src/core/events/EventBus';

describe('CvdCalculator', () => {
  it('buckets futures trades by meta.ts and emits on rollover', () => {
    const bus = createTestEventBus();
    const calc = new CvdCalculator(bus, { bucketMs: 1000, marketTypes: ['futures'] });
    const outputs: MarketCvdEvent[] = [];
    bus.subscribe('market:cvd_futures', (evt) => outputs.push(evt));
    calc.start();

    const makeTrade = (ts: number, side: TradeEvent['side'], size: number): TradeEvent => ({
      symbol: 'BTCUSDT',
      streamId: 'binance.usdm.public',
      side,
      price: 100,
      size,
      tradeTs: ts - 1,
      exchangeTs: ts - 1,
      marketType: 'futures',
      meta: createMeta('market', { ts }),
    });

    bus.publish('market:trade', makeTrade(1000, 'Buy', 2));
    bus.publish('market:trade', makeTrade(1500, 'Sell', 1));
    bus.publish('market:trade', makeTrade(2100, 'Buy', 1));

    expect(outputs).toHaveLength(1);
    const evt = outputs[0];
    expect(evt.bucketStartTs).toBe(1000);
    expect(evt.bucketEndTs).toBe(2000);
    expect(evt.bucketSizeMs).toBe(1000);
    expect(evt.cvdDelta).toBe(1);
    expect(evt.cvdTotal).toBe(1);
    expect(evt.unit).toBe('base');
    expect(evt.meta.ts).toBe(2000);
    expect(evt.marketType).toBe('futures');

    calc.stop();
  });

  it('emits spot buckets separately', () => {
    const bus = createTestEventBus();
    const calc = new CvdCalculator(bus, { bucketMs: 1000, marketTypes: ['spot'] });
    const outputs: MarketCvdEvent[] = [];
    bus.subscribe('market:cvd_spot', (evt) => outputs.push(evt));
    calc.start();

    const makeTrade = (ts: number, side: TradeEvent['side'], size: number): TradeEvent => ({
      symbol: 'BTCUSDT',
      streamId: 'binance.spot.public',
      side,
      price: 50,
      size,
      tradeTs: ts,
      exchangeTs: ts,
      marketType: 'spot',
      meta: createMeta('market', { ts }),
    });

    bus.publish('market:trade', makeTrade(3000, 'Buy', 3));
    bus.publish('market:trade', makeTrade(3200, 'Sell', 1));
    bus.publish('market:trade', makeTrade(4100, 'Sell', 2));

    expect(outputs).toHaveLength(1);
    const evt = outputs[0];
    expect(evt.bucketStartTs).toBe(3000);
    expect(evt.bucketEndTs).toBe(4000);
    expect(evt.bucketSizeMs).toBe(1000);
    expect(evt.cvdDelta).toBe(2);
    expect(evt.cvdTotal).toBe(2);
    expect(evt.unit).toBe('base');
    expect(evt.meta.ts).toBe(4000);

    calc.stop();
  });
});
