import { describe, expect, it } from 'vitest';
import { computeConfidenceScore, CONFIDENCE_FORMULA_VERSION, getSourceTrustAdjustments } from '../src/core/confidence';

describe('core confidence formula', () => {
  it('uses expectedSources ratio when provided', () => {
    const score = computeConfidenceScore({ freshSourcesCount: 2, expectedSources: 4 });
    expect(score).toBeCloseTo(0.5, 5);
  });

  it('applies deterministic penalties', () => {
    const score = computeConfidenceScore({
      freshSourcesCount: 4,
      expectedSources: 4,
      mismatchDetected: true,
      gapDetected: true,
      sequenceBroken: true,
      lagDetected: true,
    });
    const expected = 1 * 0.5 * 0.7 * 0.5 * 0.8;
    expect(score).toBeCloseTo(expected, 6);
  });

  it('clamps to 0..1 and is stable across input order', () => {
    const a = computeConfidenceScore({
      freshSourcesCount: 1,
      expectedSources: 2,
      outlierDetected: true,
      mismatchDetected: false,
    });
    const b = computeConfidenceScore({
      expectedSources: 2,
      outlierDetected: true,
      mismatchDetected: false,
      freshSourcesCount: 1,
    });
    expect(a).toBeCloseTo(b, 8);
    expect(a).toBeGreaterThanOrEqual(0);
    expect(a).toBeLessThanOrEqual(1);
  });

  it('exposes formula version', () => {
    expect(CONFIDENCE_FORMULA_VERSION).toBe('v1');
  });

  it('applies trust calibration caps and penalties deterministically', () => {
    const a = getSourceTrustAdjustments(['okx.public.swap', 'bybit.public.linear.v5'], 'liquidation');
    const b = getSourceTrustAdjustments(['bybit.public.linear.v5', 'okx.public.swap'], 'liquidation');

    expect(a.sourceCap).toBe(0.7);
    expect(a.sourcePenalty).toBeCloseTo(0.9, 6);
    expect(a.reasons).toEqual(['BYBIT_BANKRUPTCY_PRICE', 'OKX_LIQUIDATIONS_LIMITED']);
    expect(a).toEqual(b);

    const trade = getSourceTrustAdjustments(['binance.usdm.public', 'bybit.public.linear.v5'], 'trade');
    expect(trade.sourcePenalty).toBeCloseTo(0.95, 6);
  });

  it('respects sourceCap in confidence score', () => {
    const score = computeConfidenceScore({
      freshSourcesCount: 2,
      expectedSources: 2,
      sourceCap: 0.7,
    });
    expect(score).toBe(0.7);
  });
});
