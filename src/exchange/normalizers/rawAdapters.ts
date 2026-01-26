import type {
    CandleRawEvent,
    FundingRawEvent,
    IndexPriceRawEvent,
    LiquidationRawEvent,
    MarkPriceRawEvent,
    OpenInterestRawEvent,
    OrderbookDeltaRawEvent,
    OrderbookSnapshotRawEvent,
    RawLevel,
    RawSide,
    TradeRawEvent,
    VenueId,
} from '../../core/events/EventBus';
import { createMeta, type EventMeta, type MarketType } from '../../core/events/EventBus';
import { toCanonicalSymbol } from '../../core/market/symbolMapper';

const toSide = (value: string | undefined): RawSide | undefined => {
    if (!value) return undefined;
    const v = value.toLowerCase();
    if (v === 'buy' || v === 'b') return 'buy';
    if (v === 'sell' || v === 's') return 'sell';
    return undefined;
};

const toLevels = (entries: unknown): RawLevel[] => {
    if (!Array.isArray(entries)) return [];
    const out: RawLevel[] = [];
    for (const row of entries) {
        if (!Array.isArray(row) || row.length < 2) continue;
        const price = row[0] !== undefined ? String(row[0]) : undefined;
        const size = row[1] !== undefined ? String(row[1]) : undefined;
        if (!price || !size) continue;
        out.push([price, size]);
    }
    return out;
};

export function mapTradeRaw(
    venue: VenueId,
    venueSymbol: string,
    payload: Record<string, unknown>,
    exchangeTsMs: number,
    recvTsMs: number,
    marketType?: MarketType
): TradeRawEvent | undefined {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    const side = toSide(payload.side ? String(payload.side) : payload.S ? String(payload.S) : payload.s ? String(payload.s) : undefined);
    const price = payload.p ?? payload.price ?? payload.px ?? payload.P;
    const size = payload.v ?? payload.size ?? payload.q ?? payload.sz ?? payload.qty;
    if (!side || price === undefined || size === undefined) return undefined;

    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        price: String(price),
        size: String(size),
        side,
        tradeId: payload.i !== undefined ? String(payload.i) : payload.tradeId !== undefined ? String(payload.tradeId) : payload.a !== undefined ? String(payload.a) : undefined,
        seq: payload.seq !== undefined ? Number(payload.seq) : undefined,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapOrderbookSnapshotRaw(
    venue: VenueId,
    venueSymbol: string,
    bids: unknown,
    asks: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    sequence?: number,
    marketType?: MarketType
): OrderbookSnapshotRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        bids: toLevels(bids),
        asks: toLevels(asks),
        sequence,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapOrderbookDeltaRaw(
    venue: VenueId,
    venueSymbol: string,
    bids: unknown,
    asks: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    sequence?: number,
    prevSequence?: number,
    range?: { start: number; end: number },
    marketType?: MarketType
): OrderbookDeltaRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        bids: toLevels(bids),
        asks: toLevels(asks),
        sequence,
        prevSequence,
        range,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapCandleRaw(
    venue: VenueId,
    venueSymbol: string,
    interval: string,
    startTsMs: number,
    endTsMs: number,
    open: unknown,
    high: unknown,
    low: unknown,
    close: unknown,
    volume: unknown,
    isClosed: boolean,
    exchangeTsMs: number,
    recvTsMs: number,
    marketType?: MarketType
): CandleRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        interval,
        startTsMs,
        endTsMs,
        open: String(open),
        high: String(high),
        low: String(low),
        close: String(close),
        volume: String(volume),
        isClosed,
        exchangeTsMs,
        recvTsMs,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapMarkPriceRaw(
    venue: VenueId,
    venueSymbol: string,
    markPrice: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    marketType?: MarketType
): MarkPriceRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        markPrice: String(markPrice),
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapIndexPriceRaw(
    venue: VenueId,
    venueSymbol: string,
    indexPrice: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    marketType?: MarketType
): IndexPriceRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        indexPrice: String(indexPrice),
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapFundingRaw(
    venue: VenueId,
    venueSymbol: string,
    fundingRate: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    nextFundingTsMs?: number,
    marketType?: MarketType
): FundingRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        fundingRate: String(fundingRate),
        nextFundingTsMs,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapOpenInterestRaw(
    venue: VenueId,
    venueSymbol: string,
    openInterest: unknown,
    exchangeTsMs: number,
    recvTsMs: number,
    openInterestUsd?: unknown,
    marketType?: MarketType
): OpenInterestRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        openInterest: String(openInterest),
        openInterestUsd: openInterestUsd !== undefined ? String(openInterestUsd) : undefined,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function mapLiquidationRaw(
    venue: VenueId,
    venueSymbol: string,
    exchangeTsMs: number,
    recvTsMs: number,
    payload: { side?: string; price?: unknown; size?: unknown; notionalUsd?: unknown },
    marketType?: MarketType
): LiquidationRawEvent {
    const { symbol, venueSymbol: normalizedVenueSymbol } = toCanonicalSymbol(venue, venueSymbol, marketType);
    const side = payload.side ? toSide(payload.side) : undefined;
    return {
        venue,
        symbol,
        venueSymbol: normalizedVenueSymbol,
        exchangeTsMs,
        recvTsMs,
        side,
        price: payload.price !== undefined ? String(payload.price) : undefined,
        size: payload.size !== undefined ? String(payload.size) : undefined,
        notionalUsd: payload.notionalUsd !== undefined ? String(payload.notionalUsd) : undefined,
        meta: createMeta('market', { ts: exchangeTsMs }),
    };
}

export function inheritMetaOverride(meta: EventMeta, ts: number): EventMeta {
    return { ...meta, ts };
}
