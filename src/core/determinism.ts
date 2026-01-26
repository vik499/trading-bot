export function stableSortStrings(values: string[]): string[] {
    return [...values].sort((a, b) => a.localeCompare(b));
}

export function stableRecordFromEntries<T>(entries: Array<[string, T]>): Record<string, T> {
    const sorted = [...entries].sort(([a], [b]) => a.localeCompare(b));
    const record: Record<string, T> = {};
    for (const [key, value] of sorted) {
        record[key] = value;
    }
    return record;
}
