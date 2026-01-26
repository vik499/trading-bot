export interface OpenInterestCollector {
    startForSymbol(symbol: string): void;
    stop(): void;
}
