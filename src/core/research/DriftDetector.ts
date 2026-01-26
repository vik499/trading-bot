import { logger } from '../../infra/logger';
import { m } from '../logMarkers';
import {
    createMeta,
    eventBus as defaultEventBus,
    inheritMeta,
    type EventBus,
    type FeatureVectorRecorded,
    type MarketDriftDetected,
} from '../events/EventBus';

export interface DriftDetectorOptions {
    enabled?: boolean;
    baselineSize?: number;
    windowSize?: number;
    meanDeltaThreshold?: number;
    minEmitIntervalMs?: number;
    eventBus?: EventBus;
}

/**
 * Простая заглушка DriftDetector: оценивает сдвиг среднего/дисперсии по окну
 * за счёт сравнения текущего окна с базовым baseline. По умолчанию выключена.
 */
export class DriftDetector {
    private readonly enabled: boolean;
    private readonly baselineSize: number;
    private readonly windowSize: number;
    private readonly meanDeltaThreshold: number;
    private readonly minEmitIntervalMs: number;
    private readonly bus: EventBus;

    private baseline: FeatureVectorRecorded[] = [];
    private window: FeatureVectorRecorded[] = [];
    private lastEmitAt = 0;
    private started = false;
    private unsubscribe?: () => void;

    constructor(opts: DriftDetectorOptions = {}) {
        this.enabled = opts.enabled ?? false;
        const baselineSize = opts.baselineSize ?? 50;
        const windowSize = opts.windowSize ?? 20;
        this.baselineSize = Math.max(1, baselineSize);
        this.windowSize = Math.max(1, windowSize);
        this.meanDeltaThreshold = opts.meanDeltaThreshold ?? 0.1;
        this.minEmitIntervalMs = Math.max(0, opts.minEmitIntervalMs ?? 30_000);
        this.bus = opts.eventBus ?? defaultEventBus;
    }

    start(): void {
        if (!this.enabled || this.started) return;
        const handler = (fv: FeatureVectorRecorded) => this.onFeatureVector(fv);
        this.bus.subscribe('research:featureVectorRecorded', handler);
        this.unsubscribe = () => this.bus.unsubscribe('research:featureVectorRecorded', handler);
        this.started = true;
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    onFeatureVector(fv: FeatureVectorRecorded): void {
        if (!this.enabled) return;

        if (this.baseline.length < this.baselineSize) {
            this.baseline.push(fv);
            return;
        }

        this.window.push(fv);
        if (this.window.length > this.windowSize) {
            this.window.shift();
        }

        if (this.window.length < this.windowSize) return;

        const now = fv.meta.ts;
        if (now - this.lastEmitAt < this.minEmitIntervalMs) return;

        const stats = this.computeStats();
        const severityScore = this.calculateSeverity(stats);
        if (severityScore < this.meanDeltaThreshold) return;

        const severity: MarketDriftDetected['severity'] = severityScore > this.meanDeltaThreshold * 3 ? 'high' : severityScore > this.meanDeltaThreshold * 1.5 ? 'medium' : 'low';

        const payload: MarketDriftDetected = {
            meta: inheritMeta(fv.meta, 'research'),
            symbol: fv.symbol,
            window: { currentSize: this.window.length, baselineSize: this.baseline.length },
            stats,
            severity,
        };

        this.bus.publish('analytics:marketDriftDetected', payload);
        logger.info(m('ok', `[DriftDetector] drift detected for ${fv.symbol}: severity=${severity}`));
        this.lastEmitAt = now;
    }

    private computeStats(): Record<string, { meanDelta?: number; varDelta?: number }> {
        const baselineAgg = this.aggregate(this.baseline);
        const windowAgg = this.aggregate(this.window);
        const keys = new Set([...Object.keys(baselineAgg), ...Object.keys(windowAgg)]);
        const stats: Record<string, { meanDelta?: number; varDelta?: number }> = {};

        for (const key of keys) {
            const base = baselineAgg[key];
            const curr = windowAgg[key];
            if (!base || !curr) continue;
            const baseMean = base.mean;
            const currMean = curr.mean;
            const baseVar = base.var;
            const currVar = curr.var;
            stats[key] = {
                meanDelta: baseMean === 0 ? currMean : (currMean - baseMean) / Math.max(Math.abs(baseMean), 1e-9),
                varDelta: baseVar === 0 ? currVar : (currVar - baseVar) / Math.max(Math.abs(baseVar), 1e-9),
            };
        }

        return stats;
    }

    private aggregate(samples: FeatureVectorRecorded[]): Record<string, { mean: number; var: number }> {
        const sum: Record<string, number> = {};
        const count: Record<string, number> = {};
        const values: Record<string, number[]> = {};

        for (const s of samples) {
            for (const [k, v] of Object.entries(s.features)) {
                if (!Number.isFinite(v)) continue;
                sum[k] = (sum[k] ?? 0) + v;
                count[k] = (count[k] ?? 0) + 1;
                (values[k] ??= []).push(v);
            }
        }

        const stats: Record<string, { mean: number; var: number }> = {};
        for (const [k, vals] of Object.entries(values)) {
            const n = count[k] ?? 0;
            if (n === 0) continue;
            const mean = sum[k] / n;
            const variance = vals.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / n;
            stats[k] = { mean, var: variance };
        }
        return stats;
    }

    private calculateSeverity(stats: Record<string, { meanDelta?: number; varDelta?: number }>): number {
        let maxDelta = 0;
        for (const entry of Object.values(stats)) {
            const meanDelta = Math.abs(entry.meanDelta ?? 0);
            const varDelta = Math.abs(entry.varDelta ?? 0);
            maxDelta = Math.max(maxDelta, meanDelta, varDelta * 0.5);
        }
        return maxDelta;
    }
}
