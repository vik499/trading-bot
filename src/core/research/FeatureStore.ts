import { inheritMeta, eventBus as defaultEventBus, type AnalyticsFeaturesEvent, type EventBus, type FeatureVectorRecorded } from '../events/EventBus';

export interface FeatureStore {
    recordFeatureVector(event: FeatureVectorRecorded): Promise<void> | void;
    start?(): void;
    stop?(): void;
}

export class InMemoryFeatureStore implements FeatureStore {
    private readonly buffer: FeatureVectorRecorded[] = [];
    private readonly limit: number;
    private readonly bus: EventBus;
    private started = false;
    private unsubscribe?: () => void;

    constructor(limit = 500, bus: EventBus = defaultEventBus) {
        this.limit = Math.max(1, limit);
        this.bus = bus;
    }

    recordFeatureVector(event: FeatureVectorRecorded): void {
        this.buffer.push(event);
        if (this.buffer.length > this.limit) {
            this.buffer.shift();
        }
    }

    start(): void {
        if (this.started) return;
        const handler = (features: AnalyticsFeaturesEvent) => {
            const featuresMap: Record<string, number> = {};
            if (Number.isFinite(features.lastPrice)) featuresMap.lastPrice = features.lastPrice;
            if (features.return1 !== undefined && Number.isFinite(features.return1)) featuresMap.return1 = features.return1;
            if (features.sma20 !== undefined && Number.isFinite(features.sma20)) featuresMap.sma20 = features.sma20;
            if (features.volatility !== undefined && Number.isFinite(features.volatility)) featuresMap.volatility = features.volatility;
            if (features.momentum !== undefined && Number.isFinite(features.momentum)) featuresMap.momentum = features.momentum;

            const recorded: FeatureVectorRecorded = {
                symbol: features.symbol,
                featureVersion: 'v1',
                features: featuresMap,
                meta: inheritMeta(features.meta, 'research', { ts: features.meta.ts }),
            };
            this.recordFeatureVector(recorded);
            this.bus.publish('research:featureVectorRecorded', recorded);
        };

        this.bus.subscribe('analytics:features', handler);
        this.unsubscribe = () => this.bus.unsubscribe('analytics:features', handler);
        this.started = true;
    }

    stop(): void {
        this.unsubscribe?.();
        this.unsubscribe = undefined;
        this.started = false;
    }

    snapshot(): FeatureVectorRecorded[] {
        return [...this.buffer];
    }
}
