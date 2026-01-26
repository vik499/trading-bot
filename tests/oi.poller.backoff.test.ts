import { describe, it, expect, vi } from 'vitest';
import { BybitDerivativesRestPoller } from '../src/exchange/bybit/restClient';
import { createTestEventBus } from '../src/core/events/testing';
import { logger } from '../src/infra/logger';

describe('BybitDerivativesRestPoller OI backoff', () => {
  it('skips polling during backoff window after errors', async () => {
    const restClient = {
      fetchOpenInterest: vi.fn().mockRejectedValue(new Error('boom')),
      fetchFundingRate: vi.fn().mockResolvedValue({ fundingRate: 0.01, ts: 1700 }),
    };
    const poller = new BybitDerivativesRestPoller(restClient as any, createTestEventBus(), {
      oiIntervalMs: 10_000,
      oiIntervalTime: '5min',
    });

    const warnSpy = vi.spyOn(logger, 'warn').mockImplementation(() => undefined);
    const nowSpy = vi.spyOn(Date, 'now');
    let now = 1_000;
    nowSpy.mockImplementation(() => now);

    await (poller as any).pollOpenInterest('BTCUSDT');
    expect(restClient.fetchOpenInterest).toHaveBeenCalledTimes(1);

    await (poller as any).pollOpenInterest('BTCUSDT');
    expect(restClient.fetchOpenInterest).toHaveBeenCalledTimes(1);

    now += 1_000_000;
    await (poller as any).pollOpenInterest('BTCUSDT');
    expect(restClient.fetchOpenInterest).toHaveBeenCalledTimes(2);

    warnSpy.mockRestore();
    nowSpy.mockRestore();
  });
});
