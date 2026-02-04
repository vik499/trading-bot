export function makeDataQualityKey(topic: string, symbol: string, provider?: string): string {
    return `${topic}:${symbol}:${provider ?? 'global'}`;
}

export function makeDataQualitySourceId(topic: string, provider?: string): string {
    return provider ? `${provider}:${topic}` : `global:${topic}`;
}

export function parseDataQualitySourceId(sourceId: string): { provider?: string; topic: string } | undefined {
    const idx = sourceId.indexOf(':');
    if (idx <= 0 || idx >= sourceId.length - 1) return undefined;
    return { provider: sourceId.slice(0, idx), topic: sourceId.slice(idx + 1) };
}

