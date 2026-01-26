import { EventBus } from './EventBus';
import { sourceRegistry } from '../market/SourceRegistry';

/**
 * Create isolated EventBus for tests to avoid cross-test leakage.
 */
export function createTestEventBus(): EventBus {
  sourceRegistry.reset();
  return new EventBus();
}
