import type { MarketType, ReadinessBlock } from '../events/EventBus';
import { normalizeMarketType, normalizeSymbol } from './symbols';

export type ExpectedSourcesByBlock = Partial<Record<ReadinessBlock, string[]>>;
export type ExpectedSourcesByMarketType = Partial<Record<MarketType, ExpectedSourcesByBlock>>;

export interface ExpectedSourcesConfig {
    default?: ExpectedSourcesByMarketType;
    symbols?: Record<string, ExpectedSourcesByMarketType>;
}

export function parseExpectedSourcesConfig(raw?: string): ExpectedSourcesConfig | undefined {
    if (!raw || !raw.trim()) return undefined;
    try {
        const parsed = JSON.parse(raw) as ExpectedSourcesConfig;
        return sanitizeConfig(parsed);
    } catch {
        return undefined;
    }
}

export function resolveExpectedSources(
    config: ExpectedSourcesConfig | undefined,
    symbol: string,
    marketType: MarketType,
    block: ReadinessBlock
): string[] | undefined {
    if (!config) return undefined;
    const normalizedSymbol = normalizeSymbol(symbol);
    const normalizedMarketType = normalizeMarketType(marketType);

    const bySymbol = config.symbols?.[normalizedSymbol]?.[normalizedMarketType]?.[block];
    if (bySymbol && bySymbol.length) return bySymbol;

    const byDefault = config.default?.[normalizedMarketType]?.[block];
    if (byDefault && byDefault.length) return byDefault;

    return undefined;
}

function sanitizeConfig(input: ExpectedSourcesConfig): ExpectedSourcesConfig {
    const out: ExpectedSourcesConfig = {};
    if (input.default) {
        out.default = sanitizeByMarketType(input.default);
    }
    if (input.symbols) {
        out.symbols = {};
        for (const [symbol, cfg] of Object.entries(input.symbols)) {
            out.symbols[normalizeSymbol(symbol)] = sanitizeByMarketType(cfg);
        }
    }
    return out;
}

function sanitizeByMarketType(input: ExpectedSourcesByMarketType): ExpectedSourcesByMarketType {
    const out: ExpectedSourcesByMarketType = {};
    for (const [marketType, blocks] of Object.entries(input)) {
        const normalizedType = normalizeMarketType(marketType as MarketType);
        out[normalizedType] = sanitizeBlocks(blocks ?? {});
    }
    return out;
}

function sanitizeBlocks(input: ExpectedSourcesByBlock): ExpectedSourcesByBlock {
    const out: ExpectedSourcesByBlock = {};
    for (const [block, sources] of Object.entries(input)) {
        if (!Array.isArray(sources)) continue;
        const cleaned = sources.map((src) => String(src).trim()).filter((src) => src.length > 0);
        out[block as ReadinessBlock] = cleaned;
    }
    return out;
}
