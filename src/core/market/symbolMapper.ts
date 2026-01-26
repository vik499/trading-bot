import type { MarketType, VenueId } from '../events/EventBus';

export interface CanonicalSymbolMapping {
    symbol: string;
    venueSymbol?: string;
}

export function toCanonicalSymbol(venue: VenueId, venueSymbol: string, _marketType?: MarketType): CanonicalSymbolMapping {
    const raw = venueSymbol.trim().toUpperCase();
    if (!raw) return { symbol: raw };

    const ensure = (symbol: string): CanonicalSymbolMapping => {
        if (!/^[A-Z0-9]+$/.test(symbol)) {
            throw new Error(`[SymbolMapper] invalid canonical symbol venue=${venue} input=${venueSymbol}`);
        }
        return { symbol, venueSymbol: raw };
    };

    if (venue === 'okx') {
        const normalized = raw.replace(/-/g, '');
        const withoutSuffix = normalized.replace(/SWAP|FUTURES|PERP/g, '');
        return ensure(withoutSuffix);
    }

    if (raw.includes('/')) {
        return ensure(raw.replace(/\//g, ''));
    }

    return ensure(raw.replace(/-/g, ''));
}