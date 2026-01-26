import { describe, expect, it } from 'vitest';
import { toCanonicalSymbol } from '../src/core/market/symbolMapper';

describe('Symbol mapper invariants', () => {
  it('normalizes venue symbols to canonical form', () => {
    const okx = toCanonicalSymbol('okx', 'BTC-USDT-SWAP');
    expect(okx.symbol).toBe('BTCUSDT');
    expect(okx.venueSymbol).toBe('BTC-USDT-SWAP');

    const binance = toCanonicalSymbol('binance', 'btc/usdt');
    expect(binance.symbol).toBe('BTCUSDT');
    expect(binance.venueSymbol).toBe('BTC/USDT');

    const bybit = toCanonicalSymbol('bybit', 'eth-usdt');
    expect(bybit.symbol).toBe('ETHUSDT');
  });

  it('rejects invalid canonical symbols', () => {
    expect(() => toCanonicalSymbol('binance', 'BTC_USDT')).toThrow();
    expect(() => toCanonicalSymbol('okx', 'BTC USDT')).toThrow();
  });
});
