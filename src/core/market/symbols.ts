import type { KnownMarketType, MarketType } from '../events/EventBus';

export function normalizeSymbol(symbol: string): string {
    return symbol.trim().toUpperCase();
}

export function normalizeMarketType(marketType: KnownMarketType): KnownMarketType;
export function normalizeMarketType(marketType?: MarketType): MarketType;
export function normalizeMarketType(marketType?: MarketType): MarketType {
    return marketType ?? 'unknown';
}

export function inferMarketTypeFromStreamId(streamId?: string): MarketType | undefined {
    if (!streamId) return undefined;
    const id = streamId.toLowerCase();
    if (id.includes('spot')) return 'spot';
    if (id.includes('usdm') || id.includes('swap') || id.includes('linear') || id.includes('futures')) return 'futures';
    return undefined;
}
