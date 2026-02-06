import path from 'node:path';
import type { EventTapCounters } from '../EventTap';
import type { MarketDataReadiness, MarketReadinessSnapshot } from '../MarketDataReadiness';
import type { GlobalDataQualityMonitor, GlobalDataQualitySnapshot } from '../../globalData/GlobalDataQualityMonitor';
import { RotatingFileWriter } from '../logger/RotatingFileWriter';

export interface HealthReporterOptions {
  runId: string;
  logDir: string;
  intervalMs?: number;
  maxBytes?: number;
  maxFiles?: number;
  buildInfo?: {
    appVersion?: string;
    gitVersion?: string;
  };
  eventTap?: { getCounters: () => EventTapCounters };
  marketDataReadiness?: MarketDataReadiness;
  globalDataQuality?: GlobalDataQualityMonitor;
  getLifecycle?: () => string;
  getMode?: () => string;
  getPaused?: () => boolean;
  now?: () => number;
}

type HealthReporterResolvedOptions = {
  runId: string;
  logDir: string;
  intervalMs: number;
  maxBytes: number;
  maxFiles: number;
  buildInfo: {
    appVersion?: string;
    gitVersion?: string;
  };
  eventTap?: { getCounters: () => EventTapCounters };
  marketDataReadiness?: MarketDataReadiness;
  globalDataQuality?: GlobalDataQualityMonitor;
  getLifecycle: () => string;
  getMode: () => string;
  getPaused: () => boolean;
  now: () => number;
};

export class HealthReporter {
  private readonly opts: HealthReporterResolvedOptions;
  private readonly writer: RotatingFileWriter;
  private timer?: NodeJS.Timeout;

  constructor(options: HealthReporterOptions) {
    const intervalMs = options.intervalMs ?? 5_000;
    this.opts = {
      ...options,
      intervalMs,
      maxBytes: options.maxBytes ?? 10_485_760,
      maxFiles: options.maxFiles ?? 5,
      buildInfo: options.buildInfo ?? {},
      getLifecycle: options.getLifecycle ?? (() => 'STARTING'),
      getMode: options.getMode ?? (() => 'UNKNOWN'),
      getPaused: options.getPaused ?? (() => false),
      now: options.now ?? (() => Date.now()),
    };

    this.writer = new RotatingFileWriter(path.join(this.opts.logDir, 'health.jsonl'), {
      maxBytes: this.opts.maxBytes,
      maxFiles: this.opts.maxFiles,
    });
  }

  start(): void {
    if (this.timer || this.opts.intervalMs <= 0) return;
    this.timer = setInterval(() => {
      this.snapshot();
    }, this.opts.intervalMs);
    this.timer.unref?.();
  }

  stop(options: { finalSnapshot?: boolean } = {}): void {
    if (options.finalSnapshot) {
      this.snapshot();
    }
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.writer.close();
  }

  snapshot(): void {
    const ts = this.opts.now();
    const iso = new Date(ts).toISOString();
    const uptimeSec = Math.max(0, Math.floor(process.uptime()));
    const lifecycle = this.opts.getLifecycle();
    const mode = this.opts.getMode();
    const paused = this.opts.getPaused();

    const counters = this.opts.eventTap?.getCounters();
    const marketSummary = this.buildMarketSummary();
    const dataQuality = this.buildDataQualitySnapshot();

    const payload = {
      ts,
      iso,
      runId: this.opts.runId,
      appVersion: this.opts.buildInfo.appVersion,
      git: this.opts.buildInfo.gitVersion,
      pid: process.pid,
      uptimeSec,
      lifecycle,
      mode,
      paused,
      market: marketSummary.length ? marketSummary : undefined,
      counters,
      dataQuality,
    };

    this.writer.write(JSON.stringify(payload));
  }

  private buildMarketSummary(): MarketReadinessSnapshot[] {
    const snapshot = this.opts.marketDataReadiness?.getHealthSnapshot();
    if (!snapshot) return [];
    return [snapshot];
  }

  private buildDataQualitySnapshot(): GlobalDataQualitySnapshot | undefined {
    return this.opts.globalDataQuality?.snapshot(25);
  }
}
