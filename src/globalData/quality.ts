import type { AggregatedVenueBreakdown } from '../core/events/EventBus';

export interface QualitySummary {
    freshSourcesCount: number;
    staleSourcesDropped: string[];
    mismatchDetected: boolean;
    confidenceScore: number;
}

export function detectMismatch(
    venueBreakdown: AggregatedVenueBreakdown | undefined,
    thresholdPct = 0.1
): boolean {
    if (!venueBreakdown) return false;
    const values = Object.values(venueBreakdown).filter((value) => Number.isFinite(value));
    if (values.length < 2) return false;
    const min = Math.min(...values);
    const max = Math.max(...values);
    if (min <= 0) return false;
    return (max - min) / min >= thresholdPct;
}
