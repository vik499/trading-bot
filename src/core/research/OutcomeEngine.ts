import { createMeta, eventBus, inheritMeta, type EventMeta, type OutcomeRecorded } from '../events/EventBus';

export interface OutcomeEngineOptions {
    enabled?: boolean;
}

/**
 * OutcomeEngine собирает/выводит research outcomes. Сейчас это заглушка без
 * расчётов: даём API для безопасной публикации типизированных outcome-событий.
 */
export class OutcomeEngine {
    private readonly enabled: boolean;

    constructor(opts: OutcomeEngineOptions = {}) {
        this.enabled = opts.enabled ?? true;
    }

    /**
     * Публичный API: опубликовать готовый outcome. Можно использовать в тестах/стабах.
     */
    recordOutcome(outcome: Omit<OutcomeRecorded, 'meta'>, parentMeta?: EventMeta): void {
        if (!this.enabled) return;

        const payload: OutcomeRecorded = {
            ...outcome,
            meta: parentMeta ? inheritMeta(parentMeta, 'research') : createMeta('research'),
        };

        eventBus.publish('research:outcomeRecorded', payload);
    }
}
