import {
  MARKET_DATA_DEGRADED_REASON_ORDER,
  MARKET_DATA_WARNING_REASON_ORDER,
  type GapTelemetry,
  type MarketDataDegradedReason,
  type MarketDataWarningReason,
  type MarketReadinessStatus,
  type PriceStaleTelemetry,
  type RefPriceTelemetry,
} from '../MarketDataReadiness';

export type HealthMarketSnapshot = {
  symbol?: string;
  marketType?: string;
  status?: string;
  worstStatusInMinute?: MarketReadinessStatus;
  conf?: number;
  degradedReasons?: string[];
  reasons?: string[];
  reasonsUnionInMinute?: string[];
  warnings?: string[];
  warningsUnionInMinute?: string[];
  gapTelemetry?: GapTelemetry;
  priceStaleTelemetry?: PriceStaleTelemetry;
  refPriceTelemetry?: RefPriceTelemetry;
};

export type HealthLogEntry = {
  ts?: number;
  iso?: string;
  runId?: string;
  market?: HealthMarketSnapshot[];
};

export type HealthMarketEntry = {
  ts: number;
  minuteTs: number;
  symbol: string;
  marketType: string;
  status?: MarketReadinessStatus;
  worstStatusInMinute?: MarketReadinessStatus;
  reasons: MarketDataDegradedReason[];
  warnings: MarketDataWarningReason[];
  conf?: number;
  gapTelemetry?: GapTelemetry;
  priceStaleTelemetry?: PriceStaleTelemetry;
  refPriceTelemetry?: RefPriceTelemetry;
};

export function extractHealthMarketEntries(
  logs: HealthLogEntry[],
  filter: { symbol: string; marketType: string }
): HealthMarketEntry[] {
  const results: HealthMarketEntry[] = [];
  for (const entry of logs) {
    if (entry.ts === undefined) continue;
    const market = Array.isArray(entry.market) ? entry.market : [];
    for (const m of market) {
      const symbol = m.symbol;
      const marketType = m.marketType;
      if (!symbol || !marketType) continue;
      if (symbol !== filter.symbol || marketType !== filter.marketType) continue;
      const reasons = normalizeReasonArray(m.reasonsUnionInMinute ?? m.degradedReasons ?? m.reasons);
      const warnings = normalizeWarningArray(m.warningsUnionInMinute ?? m.warnings);
      const conf = typeof m.conf === 'number' ? m.conf : undefined;
      const gapTelemetry = m.gapTelemetry;
      const priceStaleTelemetry = m.priceStaleTelemetry;
      const refPriceTelemetry = m.refPriceTelemetry;
      results.push({
        ts: entry.ts,
        minuteTs: Math.floor(entry.ts / 60000) * 60000,
        symbol,
        marketType,
        status: normalizeStatus(m.status),
        worstStatusInMinute: normalizeStatus(m.worstStatusInMinute),
        reasons,
        warnings,
        conf,
        gapTelemetry,
        priceStaleTelemetry,
        refPriceTelemetry,
      });
    }
  }
  return results;
}

export function countReasonsWarnings(entries: HealthMarketEntry[]): {
  reasonCounts: Record<string, number>;
  warningCounts: Record<string, number>;
} {
  const reasonCounts: Record<string, number> = {};
  const warningCounts: Record<string, number> = {};
  for (const entry of entries) {
    for (const reason of entry.reasons) {
      reasonCounts[reason] = (reasonCounts[reason] ?? 0) + 1;
    }
    for (const warning of entry.warnings) {
      warningCounts[warning] = (warningCounts[warning] ?? 0) + 1;
    }
  }
  return { reasonCounts, warningCounts };
}

export type DegradedMinuteRow = {
  minuteTs: number;
  symbol: string;
  marketType: string;
  status?: MarketReadinessStatus;
  reasons: MarketDataDegradedReason[];
  warnings: MarketDataWarningReason[];
  conf?: number;
  gapTelemetry?: GapTelemetry;
  priceStaleTelemetry?: PriceStaleTelemetry;
  refPriceTelemetry?: RefPriceTelemetry;
};

export function summarizeDegradedMinutes(entries: HealthMarketEntry[]): DegradedMinuteRow[] {
  const byMinute = new Map<number, HealthMarketEntry>();
  for (const entry of entries) {
    const existing = byMinute.get(entry.minuteTs);
    if (!existing || entry.ts > existing.ts) {
      byMinute.set(entry.minuteTs, entry);
    }
  }
  const rows: DegradedMinuteRow[] = [];
  for (const entry of byMinute.values()) {
    const minuteStatus = entry.worstStatusInMinute ?? entry.status;
    if (minuteStatus !== 'DEGRADED' && minuteStatus !== 'NO_DATA') continue;
    rows.push({
      minuteTs: entry.minuteTs,
      symbol: entry.symbol,
      marketType: entry.marketType,
      status: minuteStatus,
      reasons: entry.reasons,
      warnings: entry.warnings,
      conf: entry.conf,
      gapTelemetry: entry.reasons.includes('GAPS_DETECTED') ? entry.gapTelemetry : undefined,
      priceStaleTelemetry: entry.reasons.includes('PRICE_STALE') ? entry.priceStaleTelemetry : undefined,
      refPriceTelemetry: entry.reasons.includes('NO_VALID_REF_PRICE') ? entry.refPriceTelemetry : undefined,
    });
  }
  return rows.sort((a, b) => a.minuteTs - b.minuteTs);
}

export type MismatchLogEntry = {
  mismatchDetected?: boolean;
  mismatchType?: string;
  tsEvent?: number;
  bucketEndTs?: number;
  bucketStartTs?: number;
  ts?: number;
};

export type MismatchMinuteStat = {
  minuteTs: number;
  hardPct: number;
  softPct: number;
  dominantMismatchType: string | null;
  total: number;
};

export function groupMismatchByMinute(entries: MismatchLogEntry[]): MismatchMinuteStat[] {
  const byMinute = new Map<number, MismatchLogEntry[]>();
  for (const entry of entries) {
    const ts = pickMismatchTimestamp(entry);
    if (ts === null) continue;
    const minuteTs = Math.floor(ts / 60000) * 60000;
    const bucket = byMinute.get(minuteTs) ?? [];
    bucket.push(entry);
    byMinute.set(minuteTs, bucket);
  }

  const stats: MismatchMinuteStat[] = [];
  for (const [minuteTs, bucket] of byMinute.entries()) {
    const total = bucket.length;
    let hardCount = 0;
    let softCount = 0;
    const typeCounts = new Map<string, number>();
    for (const entry of bucket) {
      if (entry.mismatchDetected === true) hardCount += 1;
      if (entry.mismatchType && entry.mismatchType !== 'NONE') softCount += 1;
      if (entry.mismatchType) {
        typeCounts.set(entry.mismatchType, (typeCounts.get(entry.mismatchType) ?? 0) + 1);
      }
    }
    let dominant: string | null = null;
    let dominantCount = -1;
    for (const [key, count] of typeCounts.entries()) {
      if (count > dominantCount || (count === dominantCount && key < (dominant ?? key))) {
        dominant = key;
        dominantCount = count;
      }
    }
    stats.push({
      minuteTs,
      hardPct: total > 0 ? hardCount / total : 0,
      softPct: total > 0 ? softCount / total : 0,
      dominantMismatchType: dominant,
      total,
    });
  }

  return stats.sort((a, b) => a.minuteTs - b.minuteTs);
}

function pickMismatchTimestamp(entry: MismatchLogEntry): number | null {
  const candidates = [entry.tsEvent, entry.bucketEndTs, entry.bucketStartTs, entry.ts];
  for (const value of candidates) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
  }
  return null;
}

function normalizeStatus(value: unknown): MarketReadinessStatus | undefined {
  if (value !== 'READY' && value !== 'WARMING' && value !== 'DEGRADED' && value !== 'NO_DATA') {
    return undefined;
  }
  return value;
}

function normalizeReasonArray(value: unknown): MarketDataDegradedReason[] {
  if (!Array.isArray(value)) return [];
  const reasonSet = new Set<MarketDataDegradedReason>();
  for (const item of value) {
    const normalized = normalizeReasonValue(item);
    if (normalized !== undefined) reasonSet.add(normalized);
  }
  return MARKET_DATA_DEGRADED_REASON_ORDER.filter((reason) => reasonSet.has(reason));
}

function normalizeReasonValue(value: unknown): MarketDataDegradedReason | undefined {
  if (value === 'NO_REF_PRICE') return 'NO_VALID_REF_PRICE';
  if (typeof value === 'string' && DEGRADED_REASON_SET.has(value as MarketDataDegradedReason)) {
    return value as MarketDataDegradedReason;
  }
  return undefined;
}

function normalizeWarningArray(value: unknown): MarketDataWarningReason[] {
  if (!Array.isArray(value)) return [];
  const warningSet = new Set<MarketDataWarningReason>();
  for (const item of value) {
    if (typeof item === 'string' && WARNING_REASON_SET.has(item as MarketDataWarningReason)) {
      warningSet.add(item as MarketDataWarningReason);
    }
  }
  return MARKET_DATA_WARNING_REASON_ORDER.filter((warning) => warningSet.has(warning));
}

const DEGRADED_REASON_SET = new Set<MarketDataDegradedReason>(MARKET_DATA_DEGRADED_REASON_ORDER);
const WARNING_REASON_SET = new Set<MarketDataWarningReason>(MARKET_DATA_WARNING_REASON_ORDER);
