# Research Roadmap (Next Steps, Not Implemented)

This document captures the next research and risk steps. No code changes here yet.

## 1) HMM Regime Engine
- Add an analytics module that consumes MTF features + flow/liquidity + volatility.
- Output regime probabilities and a stable regime label.
- Consider publishing `analytics:regime_hmm` or integrating into MarketContextBuilder.
- Keep deterministic meta.ts based on event streams.

## 2) Matrix Profile Pattern Miner
- Add a research module (PatternMiner) for kline series or feature vectors.
- Output `research:patternFound` with match scores and window stats.
- Store artifacts via FeatureStore/Journal for replayable evaluation.

## 3) Walk-forward Validation in Replay
- Extend ReplayRunner to slice train/test windows per run.
- Enforce no leakage by design (strict time ordering).
- Emit run-level metrics for each window.

## 4) Risk v2
- Volatility targeting with regime-aware sizing.
- Kill switch on max drawdown (capital preservation first).
- Multi-TF stop logic and cooldown after adverse regimes.
