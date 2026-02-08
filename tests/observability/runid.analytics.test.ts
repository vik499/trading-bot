import { describe, expect, it } from 'vitest';
import {
  countReasonsWarnings,
  extractHealthMarketEntries,
  groupMismatchByMinute,
  summarizeDegradedMinutes,
  type HealthLogEntry,
  type MismatchLogEntry,
} from '../../src/observability/analytics';
import type { GapTelemetry, PriceStaleTelemetry, RefPriceTelemetry } from '../../src/observability/MarketDataReadiness';

describe('runId analytics helpers', () => {
  it('extracts reasons and warnings from health market entries', () => {
    const sample: HealthLogEntry = {
      ts: 123,
      market: [
        {
          symbol: 'BTCUSDT',
          marketType: 'futures',
          status: 'DEGRADED',
          degradedReasons: ['PRICE_STALE', 'NO_DATA'],
          warnings: ['EXCHANGE_LAG_TOO_HIGH'],
        },
      ],
    };

    const rows = extractHealthMarketEntries([sample], { symbol: 'BTCUSDT', marketType: 'futures' });
    const { reasonCounts, warningCounts } = countReasonsWarnings(rows);

    expect(reasonCounts.PRICE_STALE).toBe(1);
    expect(reasonCounts.NO_DATA).toBe(1);
    expect(warningCounts.EXCHANGE_LAG_TOO_HIGH).toBe(1);
  });

  it('computes hardPct and softPct per minute for mismatch logs', () => {
    const entries: MismatchLogEntry[] = [
      { tsEvent: 60_000, mismatchDetected: false, mismatchType: 'SIGN' },
      { tsEvent: 60_500, mismatchDetected: true, mismatchType: 'SIGN' },
      { tsEvent: 120_000, mismatchDetected: false, mismatchType: 'NONE' },
    ];

    const stats = groupMismatchByMinute(entries);
    expect(stats).toHaveLength(2);

    const first = stats[0];
    expect(first.hardPct).toBeCloseTo(0.5);
    expect(first.softPct).toBeCloseTo(1);
    expect(first.dominantMismatchType).toBe('SIGN');

    const second = stats[1];
    expect(second.hardPct).toBeCloseTo(0);
    expect(second.softPct).toBeCloseTo(0);
    expect(second.dominantMismatchType).toBe('NONE');
  });

  it('summarizes degraded minutes and attaches telemetry only when relevant', () => {
    const gapTelemetry: GapTelemetry = {
      markersCountBySource: {
        onGap: 1,
        onSequenceGap: 0,
        detectTickerQuality: 0,
        detectOrderbookQuality: 1,
        detectOiQuality: 0,
        detectFundingQuality: 0,
      },
      markersTop: [{ marker: 'detectOrderbookQuality', count: 1 }],
      inputGapStats: {
        samples: 1,
        maxMs: 6000,
        p95Ms: 6000,
        overThresholdCount: 1,
        seqGapCount: 0,
      },
    };
    const priceStaleTelemetry: PriceStaleTelemetry = {
      bucketTs: 120_000,
      priceTsEvent: 119_000,
      priceBucketTs: 120_000,
      bucketDeltaMs: 0,
      ageMs: 1000,
      staleMs: 1000,
      sourcesUsed: ['binance.usdm.public'],
    };
    const refPriceTelemetry: RefPriceTelemetry = {
      bucketTs: 120_000,
      hasPriceEvent: true,
      priceBucketMatch: false,
      sourceCounts: { used: 0, staleDropped: 1 },
    };
    const logs: HealthLogEntry[] = [
      {
        ts: 60_000,
        market: [
          {
            symbol: 'BTCUSDT',
            marketType: 'futures',
            status: 'DEGRADED',
            degradedReasons: ['GAPS_DETECTED'],
            gapTelemetry,
          },
        ],
      },
      {
        ts: 120_000,
        market: [
          {
            symbol: 'BTCUSDT',
            marketType: 'futures',
            status: 'DEGRADED',
            degradedReasons: ['PRICE_STALE', 'NO_REF_PRICE'],
            priceStaleTelemetry,
            refPriceTelemetry,
          },
        ],
      },
    ];

    const entries = extractHealthMarketEntries(logs, { symbol: 'BTCUSDT', marketType: 'futures' });
    const rows = summarizeDegradedMinutes(entries);

    expect(rows).toHaveLength(2);
    expect(rows[0].gapTelemetry).toBeTruthy();
    expect(rows[0].priceStaleTelemetry).toBeUndefined();
    expect(rows[0].refPriceTelemetry).toBeUndefined();
    expect(rows[1].gapTelemetry).toBeUndefined();
    expect(rows[1].priceStaleTelemetry).toBeTruthy();
    expect(rows[1].refPriceTelemetry).toBeTruthy();
  });
});
