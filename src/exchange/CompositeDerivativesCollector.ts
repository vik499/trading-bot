import type { OpenInterestCollector } from '../core/market/OpenInterestCollector';

export class CompositeDerivativesCollector implements OpenInterestCollector {
    constructor(private readonly collectors: OpenInterestCollector[]) {}

    startForSymbol(symbol: string): void {
        for (const collector of this.collectors) {
            collector.startForSymbol(symbol);
        }
    }

    stop(): void {
        for (const collector of this.collectors) {
            collector.stop();
        }
    }
}
