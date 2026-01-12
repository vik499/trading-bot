import { inheritMeta, eventBus as defaultEventBus, type EventBus, type FeaturesComputed, type FeatureVectorRecorded } from '../events/EventBus';

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
        const handler = (features: FeaturesComputed) => {
            const recorded: FeatureVectorRecorded = {
                symbol: features.symbol,
                timeframe: features.timeframe,
                featureVersion: 'v1',
                features: features.features,
                meta: inheritMeta(features.meta, 'research'),
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
