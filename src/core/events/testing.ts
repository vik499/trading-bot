import { EventBus } from './EventBus';

/**
 * Create isolated EventBus for tests to avoid cross-test leakage.
 */
export function createTestEventBus(): EventBus {
  return new EventBus();
}
