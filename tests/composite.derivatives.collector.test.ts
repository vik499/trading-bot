import { describe, it, expect, vi } from 'vitest';
import { CompositeDerivativesCollector } from '../src/exchange/CompositeDerivativesCollector';

describe('CompositeDerivativesCollector', () => {
  it('forwards start/stop to all collectors', () => {
    const first = { startForSymbol: vi.fn(), stop: vi.fn() };
    const second = { startForSymbol: vi.fn(), stop: vi.fn() };
    const composite = new CompositeDerivativesCollector([first, second]);

    composite.startForSymbol('BTCUSDT');
    expect(first.startForSymbol).toHaveBeenCalledWith('BTCUSDT');
    expect(second.startForSymbol).toHaveBeenCalledWith('BTCUSDT');

    composite.stop();
    expect(first.stop).toHaveBeenCalled();
    expect(second.stop).toHaveBeenCalled();
  });
});
