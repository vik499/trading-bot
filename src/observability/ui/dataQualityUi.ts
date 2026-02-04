export function formatDataQualityKeyLabel(
    keys: string[],
    fallback: string,
    opts: { maxKeys?: number } = {}
): string {
    const maxKeys = Math.max(1, Math.floor(opts.maxKeys ?? 2));
    if (!keys.length) return fallback;
    const limited = keys.slice(0, maxKeys);
    if (keys.length <= maxKeys) return limited.join(', ');
    return `${limited.join(', ')}, ...(+${keys.length - maxKeys})`;
}

export function formatDataSourceDegradedUiLine(opts: {
    appTag: string;
    keys: string[];
    sourceId: string;
    reason: string;
}): string {
    const label = formatDataQualityKeyLabel(opts.keys, opts.sourceId);
    return `${opts.appTag} Данные: источник ${label} деградировал (${opts.reason})`;
}

export function formatDataSourceRecoveredUiLine(opts: { appTag: string; keys: string[]; sourceId: string }): string {
    const label = formatDataQualityKeyLabel(opts.keys, opts.sourceId);
    return `${opts.appTag} Данные: источник ${label} восстановлен`;
}

