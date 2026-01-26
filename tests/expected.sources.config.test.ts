import { describe, expect, it } from 'vitest';
import { parseExpectedSourcesConfig, resolveExpectedSources } from '../src/core/market/expectedSources';

describe('Expected sources config', () => {
  it('resolves symbol-specific overrides before defaults', () => {
    const raw = JSON.stringify({
      default: {
        futures: {
          price: ['bybit.public.linear.v5'],
        },
      },
      symbols: {
        BTCUSDT: {
          futures: {
            price: ['binance.usdm.public', 'okx.public.swap'],
          },
        },
      },
    });

    const config = parseExpectedSourcesConfig(raw);
    const resolved = resolveExpectedSources(config, 'btcusdt', 'futures', 'price');
    expect(resolved).toEqual(['binance.usdm.public', 'okx.public.swap']);
  });

  it('falls back to default when symbol override missing', () => {
    const raw = JSON.stringify({
      default: {
        futures: {
          flow: ['bybit.public.linear.v5'],
        },
      },
    });

    const config = parseExpectedSourcesConfig(raw);
    const resolved = resolveExpectedSources(config, 'ETHUSDT', 'futures', 'flow');
    expect(resolved).toEqual(['bybit.public.linear.v5']);
  });
});
