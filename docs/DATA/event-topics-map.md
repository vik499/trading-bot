# Event Topics Map

Source of truth: `src/core/events/EventBus.ts` (BotEventMap).
Publishers/Subscribers: static scan of `src/**` for `publish('topic')` and `subscribe('topic')` / `on('topic')` usage.

| Topic | Publishers | Subscribers | Notes (meta rules / determinism / replay impact) |
|---|---|---|---|
| market:ticker | src/exchange/bybit/wsClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/FeatureEngine.ts<br>src/apps/live-bot.ts<br>src/control/orchestrator.ts<br>src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:kline | src/exchange/marketGateway.ts<br>src/exchange/bybit/wsClient.ts<br>src/exchange/binance/wsClient.ts<br>src/exchange/okx/wsClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/FeatureEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only. OKX klines routed via business WS (public WS excludes candle channels). |
| market:trade | src/exchange/bybit/wsClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/FlowEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:orderbook_l2_snapshot | src/exchange/bybit/wsClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/LiquidityEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:orderbook_l2_delta | src/exchange/bybit/wsClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/LiquidityEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:oi | src/exchange/bybit/restClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/FlowEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:funding | src/exchange/bybit/restClient.ts<br>src/replay/JournalReplayRunner.ts | src/analytics/FlowEngine.ts<br>src/observability/EventTap.ts<br>src/storage/eventJournal.ts | Replay deterministic; Meta rule: use event.meta only |
| market:oi_agg | src/globalData/GlobalDataGateway.ts<br>src/globalData/OpenInterestAggregator.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| market:funding_agg | src/globalData/GlobalDataGateway.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| market:liquidations_agg | src/globalData/GlobalDataGateway.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| market:volume_agg | src/globalData/GlobalDataGateway.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| market:cvd_agg | src/globalData/GlobalDataGateway.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| market:price_index | src/globalData/GlobalDataGateway.ts | src/analytics/MarketViewBuilder.ts<br>src/globalData/GlobalDataQualityMonitor.ts<br>src/observability/EventTap.ts | Global aggregated; Meta rule: use event.meta only |
| control:command | src/apps/live-bot.ts<br>src/cli/cliApp.ts<br>src/hard-tests/hard-test.ts | src/cli/cliApp.ts<br>src/control/orchestrator.ts<br>src/hard-tests/hard-test.ts | — |
| control:state | src/control/orchestrator.ts | src/apps/live-bot.ts<br>src/cli/cliApp.ts<br>src/risk/RiskManager.ts<br>src/hard-tests/hard-test.ts | — |
| analytics:features | src/analytics/FeatureEngine.ts | src/analytics/MarketContextBuilder.ts<br>src/core/research/FeatureStore.ts<br>src/execution/PaperExecutionEngine.ts<br>src/observability/EventTap.ts<br>src/strategy/StrategyEngine.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:context | src/analytics/MarketContextBuilder.ts | src/observability/EventTap.ts<br>src/risk/RiskManager.ts<br>src/strategy/StrategyEngine.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:ready | src/analytics/FeatureEngine.ts<br>src/analytics/MarketContextBuilder.ts | src/analytics/MarketContextBuilder.ts<br>src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:flow | src/analytics/FlowEngine.ts | src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:liquidity | src/analytics/LiquidityEngine.ts | src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:market_view | src/analytics/MarketViewBuilder.ts | src/analytics/RegimeEngineV1.ts<br>src/research/PatternMiner.ts<br>src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:regime | src/analytics/RegimeEngineV1.ts | src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| analytics:regime_explain | src/analytics/RegimeEngineV1.ts | src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| strategy:intent | src/strategy/StrategyEngine.ts | src/observability/EventTap.ts<br>src/risk/RiskManager.ts | Replay deterministic; Meta rule: use event.meta only |
| strategy:signal | (NO_PUBLISHER) | (NO_SUBSCRIBER) | Replay deterministic; Meta rule: use event.meta only |
| strategy:state | src/strategy/StrategyEngine.ts | (NO_SUBSCRIBER) | Replay deterministic; Meta rule: use event.meta only |
| risk:approved_intent | src/risk/RiskManager.ts | src/execution/PaperExecutionEngine.ts<br>src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| risk:rejected_intent | src/risk/RiskManager.ts | src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| exec:paper_fill | src/execution/PaperExecutionEngine.ts | src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts<br>src/portfolio/PortfolioManager.ts | Replay deterministic; Meta rule: use event.meta only |
| portfolio:update | src/portfolio/PortfolioManager.ts | src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts | Replay deterministic; Meta rule: use event.meta only |
| portfolio:snapshot | (NO_PUBLISHER) | (NO_SUBSCRIBER) | Replay deterministic; Meta rule: use event.meta only |
| state:snapshot_requested | src/cli/cliApp.ts | src/state/SnapshotCoordinator.ts | — |
| state:snapshot_written | src/state/SnapshotCoordinator.ts | (NO_SUBSCRIBER) | — |
| state:recovery_requested | src/cli/cliApp.ts | src/state/SnapshotCoordinator.ts | — |
| state:recovery_loaded | src/state/SnapshotCoordinator.ts | (NO_SUBSCRIBER) | — |
| state:recovery_failed | src/state/SnapshotCoordinator.ts | (NO_SUBSCRIBER) | — |
| metrics:backtest_summary | src/metrics/BacktestMetricsCollector.ts | src/apps/replay.ts | Replay deterministic |
| metrics:walkforward_summary | src/replay/WalkForwardRunner.ts | src/apps/replay.ts | Replay deterministic |
| market:connect | src/control/orchestrator.ts | src/exchange/marketGateway.ts | — |
| market:disconnect | (NO_PUBLISHER) | src/exchange/marketGateway.ts | — |
| market:subscribe | src/control/orchestrator.ts | src/exchange/marketGateway.ts | — |
| market:connected | src/exchange/marketGateway.ts | (NO_SUBSCRIBER) | — |
| market:disconnected | src/exchange/marketGateway.ts | (NO_SUBSCRIBER) | — |
| market:error | src/exchange/marketGateway.ts | (NO_SUBSCRIBER) | — |
| market:kline_bootstrap_requested | src/control/orchestrator.ts | src/exchange/marketGateway.ts | — |
| market:kline_bootstrap_completed | src/exchange/marketGateway.ts | src/observability/EventTap.ts | — |
| market:kline_bootstrap_failed | src/exchange/marketGateway.ts | (NO_SUBSCRIBER) | — |
| state:snapshot | (NO_PUBLISHER) | (NO_SUBSCRIBER) | — |
| state:recovery | (NO_PUBLISHER) | (NO_SUBSCRIBER) | — |
| data:gapDetected | src/storage/eventJournal.ts | (NO_SUBSCRIBER) | — |
| data:outOfOrder | src/storage/eventJournal.ts | (NO_SUBSCRIBER) | — |
| data:latencySpike | src/storage/eventJournal.ts | (NO_SUBSCRIBER) | — |
| data:duplicateDetected | src/storage/eventJournal.ts | (NO_SUBSCRIBER) | — |
| data:sourceDegraded | src/globalData/GlobalDataGateway.ts | (NO_SUBSCRIBER) | — |
| data:sourceRecovered | src/globalData/GlobalDataGateway.ts | (NO_SUBSCRIBER) | — |
| data:stale | src/globalData/GlobalDataQualityMonitor.ts | (NO_SUBSCRIBER) | — |
| data:mismatch | src/globalData/GlobalDataQualityMonitor.ts | (NO_SUBSCRIBER) | — |
| storage:writeFailed | src/storage/eventJournal.ts | src/apps/live-bot.ts | — |
| replay:started | src/replay/JournalReplayRunner.ts | src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts | — |
| replay:progress | src/replay/JournalReplayRunner.ts | src/apps/replay.ts | — |
| replay:finished | src/replay/JournalReplayRunner.ts | src/apps/replay.ts<br>src/metrics/BacktestMetricsCollector.ts<br>src/observability/EventTap.ts | — |
| replay:warning | src/replay/JournalReplayRunner.ts | src/apps/replay.ts | — |
| replay:error | src/replay/JournalReplayRunner.ts | src/apps/replay.ts | — |
| error:event | (NO_PUBLISHER) | (NO_SUBSCRIBER) | — |
| analytics:marketDriftDetected | src/core/research/DriftDetector.ts | (NO_SUBSCRIBER) | Replay deterministic |
| research:featureVectorRecorded | src/core/research/FeatureStore.ts | src/core/research/DriftDetector.ts | — |
| research:outcomeRecorded | src/core/research/OutcomeEngine.ts | src/core/research/ResearchOrchestrator.ts | — |
| research:patternFound | src/research/PatternMiner.ts | (NO_SUBSCRIBER) | — |
| research:patternStats | src/research/PatternMiner.ts | (NO_SUBSCRIBER) | — |
| research:newClusterFound | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| research:candidateScenarioBuilt | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| research:backtestCompleted | src/core/research/ResearchRunner.ts | src/core/research/ResearchOrchestrator.ts | — |
| research:scenarioApproved | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| research:scenarioRejected | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| strategy:scenarioEnabled | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| strategy:scenarioDisabled | (NO_PUBLISHER) | src/core/research/ResearchOrchestrator.ts | — |
| control:rollbackModel | (NO_PUBLISHER) | (NO_SUBSCRIBER) | — |

## Contract violations

None found.

## How generated

Static scan of `src/**/*.ts` for publish/subscribe/on topic strings, compared against `BotEventMap` in `src/core/events/EventBus.ts`.
