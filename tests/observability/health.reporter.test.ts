import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { HealthReporter } from '../../src/observability/health/HealthReporter';

describe('HealthReporter', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('writes JSONL snapshot with required fields', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bot-health-'));
    const reporter = new HealthReporter({
      runId: 'run-1',
      logDir: dir,
      intervalMs: 0,
      maxBytes: 1_000_000,
      maxFiles: 1,
      buildInfo: { appVersion: '1.2.3', gitVersion: 'abc123' },
      getLifecycle: () => 'RUNNING',
      getMode: () => 'LIVE',
      getPaused: () => false,
      now: () => 1_700_000_000_000,
    });

    reporter.snapshot();
    reporter.stop();

    const healthPath = path.join(dir, 'health.jsonl');
    const lines = fs.readFileSync(healthPath, 'utf8').trim().split('\n');
    expect(lines).toHaveLength(1);

    const payload = JSON.parse(lines[0]) as Record<string, unknown>;
    expect(payload.runId).toBe('run-1');
    expect(payload.lifecycle).toBe('RUNNING');
    expect(payload.mode).toBe('LIVE');
    expect(payload.paused).toBe(false);
    expect(payload.appVersion).toBe('1.2.3');
    expect(payload.git).toBe('abc123');
    expect(typeof payload.ts).toBe('number');
    expect(typeof payload.iso).toBe('string');
    expect(typeof payload.pid).toBe('number');
    expect(typeof payload.uptimeSec).toBe('number');
  });

  it('writes JSONL lines on interval', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bot-health-interval-'));
    const reporter = new HealthReporter({
      runId: 'run-2',
      logDir: dir,
      intervalMs: 100,
      maxBytes: 1_000_000,
      maxFiles: 1,
      now: () => Date.now(),
    });

    reporter.start();
    vi.advanceTimersByTime(350);
    reporter.stop();

    const healthPath = path.join(dir, 'health.jsonl');
    const lines = fs.readFileSync(healthPath, 'utf8').trim().split('\n');
    expect(lines.length).toBeGreaterThanOrEqual(3);
  });
});
