import { asTsMs, createMeta, eventBus, type EventBus, type WalkForwardSummary } from '../core/events/EventBus';
import { formatUtcDate } from '../core/time/utc';
import { createJournalReplayRunner, type ReplayMode } from './JournalReplayRunner';

export interface WalkForwardOptions {
    streamId: string;
    symbol: string;
    baseDir?: string;
    runId?: string;
    dateFrom: string;
    dateTo: string;
    trainDays: number;
    testDays: number;
    stepDays: number;
    mode?: ReplayMode;
    speedFactor?: number;
}

type ResolvedWalkForwardOptions = Required<Omit<WalkForwardOptions, 'runId'>> & { runId?: string };

export class WalkForwardRunner {
    private readonly bus: EventBus;
    private readonly options: ResolvedWalkForwardOptions;

    constructor(bus: EventBus = eventBus, options: WalkForwardOptions) {
        this.bus = bus;
        this.options = {
            baseDir: options.baseDir ?? process.env.BOT_JOURNAL_DIR ?? './data/journal',
            streamId: options.streamId,
            symbol: options.symbol,
            runId: options.runId,
            dateFrom: options.dateFrom,
            dateTo: options.dateTo,
            trainDays: Math.max(1, options.trainDays),
            testDays: Math.max(1, options.testDays),
            stepDays: Math.max(1, options.stepDays),
            mode: options.mode ?? 'max',
            speedFactor: options.speedFactor ?? 10,
        };
    }

    async run(): Promise<WalkForwardSummary> {
        const startTs = toUtcMs(this.options.dateFrom);
        const endTs = toUtcMs(this.options.dateTo);
        const dayMs = 86_400_000;
        const trainMs = this.options.trainDays * dayMs;
        const testMs = this.options.testDays * dayMs;
        const stepMs = this.options.stepDays * dayMs;

        let folds = 0;
        for (let cursor = startTs; cursor + trainMs + testMs <= endTs; cursor += stepMs) {
            const trainFrom = toDateString(cursor);
            const trainTo = toDateString(cursor + trainMs - dayMs);
            const testFrom = toDateString(cursor + trainMs);
            const testTo = toDateString(cursor + trainMs + testMs - dayMs);

            await this.runWindow(trainFrom, trainTo);
            await this.runWindow(testFrom, testTo);
            folds += 1;
        }

        const summary: WalkForwardSummary = {
            runId: this.options.runId ?? 'walkforward',
            symbol: this.options.symbol,
            folds,
            trainWindowMs: trainMs,
            testWindowMs: testMs,
            stepMs,
            startTs,
            endTs,
                meta: createMeta('metrics', { tsEvent: asTsMs(endTs) }),
        };
        this.bus.publish('metrics:walkforward_summary', summary);
        return summary;
    }

    private async runWindow(dateFrom: string, dateTo: string): Promise<void> {
        const runner = createJournalReplayRunner(this.bus, {
            baseDir: this.options.baseDir,
            streamId: this.options.streamId,
            symbol: this.options.symbol,
            runId: this.options.runId,
            dateFrom,
            dateTo,
            mode: this.options.mode,
            speedFactor: this.options.speedFactor,
        });
        await runner.run();
    }
}

function toUtcMs(date: string): number {
    const parsed = Date.parse(`${date}T00:00:00Z`);
    if (!Number.isFinite(parsed)) {
        throw new Error(`Invalid date: ${date}`);
    }
    return parsed;
}

function toDateString(ts: number): string {
    return formatUtcDate(ts);
}
