import { createMeta, eventBus, inheritMeta, type BacktestCompleted, type EventMeta } from '../events/EventBus';

export interface ResearchRunnerOptions {
    enabled?: boolean;
}

/**
 * Заглушка для бэктестов/реплеев. Сейчас только публикует типизированное событие
 * о завершении бэктеста, чтобы держать контракт стабильным.
 */
export class ResearchRunner {
    private readonly enabled: boolean;

    constructor(opts: ResearchRunnerOptions = {}) {
        this.enabled = opts.enabled ?? true;
    }

    publishBacktestCompleted(data: Omit<BacktestCompleted, 'meta'>, parentMeta?: EventMeta): void {
        if (!this.enabled) return;
        const payload: BacktestCompleted = {
            ...data,
            meta: parentMeta ? inheritMeta(parentMeta, 'research') : createMeta('research'),
        };
        eventBus.publish('research:backtestCompleted', payload);
    }
}
