import { describe, it, expect } from 'vitest';
import { DebouncedConsoleTransitions } from '../../src/observability/ui/DebouncedConsoleTransitions';

describe('DebouncedConsoleTransitions', () => {
  it('emits at most one message for degraded/recovered flaps within the debounce window', () => {
    let t = 0;
    const gate = new DebouncedConsoleTransitions({
      debounceMs: 1000,
      holdDownMs: 1000,
      now: () => t,
    });

    expect(gate.shouldEmitDegraded('src')).toBe(true);
    t += 100;
    expect(gate.shouldEmitRecovered('src')).toBe(false);

    t += 1000;
    expect(gate.shouldEmitRecovered('src')).toBe(true);
  });
});

