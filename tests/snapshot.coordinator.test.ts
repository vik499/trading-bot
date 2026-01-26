import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  createMeta,
  type BotEventMap,
  type EventBus,
  type RecoveryFailed,
  type RecoveryLoaded,
  type SnapshotWritten,
} from '../src/core/events/EventBus';
import { createTestEventBus } from '../src/core/events/testing';
import { SnapshotCoordinator } from '../src/state/SnapshotCoordinator';
import { PortfolioManager } from '../src/portfolio/PortfolioManager';

const tempDirs: string[] = [];

function waitForEvent<T extends keyof BotEventMap>(bus: EventBus, topic: T, timeoutMs = 1500): Promise<BotEventMap[T][0]> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      bus.unsubscribe(topic, handler as any);
      reject(new Error(`Timed out waiting for event ${topic}`));
    }, timeoutMs);

    const handler = (payload: BotEventMap[T][0]) => {
      clearTimeout(timer);
      bus.unsubscribe(topic, handler as any);
      resolve(payload);
    };

    bus.subscribe(topic, handler as any);
  });
}

async function makeTempDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'snapshot-test-'));
  tempDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => fs.rm(dir, { recursive: true, force: true })));
});

describe('SnapshotCoordinator', () => {
  it('writes snapshot file and emits state:snapshot_written', async () => {
    const tmpDir = await makeTempDir();
    const bus = createTestEventBus();
    const portfolio = new PortfolioManager(bus);
    portfolio.setState({ BTCUSDT: { qty: 1, avgPrice: 100, realizedPnl: 0, updatedAtTs: 1 } });

    const coordinator = new SnapshotCoordinator(bus, { portfolio }, { baseDir: tmpDir, runId: 't1' });
    coordinator.start();

    const targetPath = path.join(tmpDir, 'snapshot-manual.json');
    const waitWritten = waitForEvent(bus, 'state:snapshot_written');
    bus.publish('state:snapshot_requested', { meta: createMeta('system'), pathOverride: targetPath });

    const written = (await waitWritten) as SnapshotWritten;
    const raw = await fs.readFile(targetPath, 'utf8');
    const parsed = JSON.parse(raw);

    expect(written.path).toBe(targetPath);
    expect(parsed.version).toBe('1');
    expect(parsed.portfolio.BTCUSDT.qty).toBe(1);
    await expect(fs.access(`${targetPath}.tmp`)).rejects.toThrow();

    coordinator.stop();
  });

  it('loads the latest snapshot and emits state:recovery_loaded', async () => {
    const tmpDir = await makeTempDir();
    const bus = createTestEventBus();
    const portfolio = new (class {
      private state: unknown = {};
      getState() {
        return this.state;
      }
      setState(next: unknown) {
        this.state = next;
      }
    })();

    const older = path.join(tmpDir, 'snapshot-test-2024-01-01T00-00-00.000Z.json');
    const newer = path.join(tmpDir, 'snapshot-test-2025-01-01T00-00-00.000Z.json');
    await fs.writeFile(
      older,
      JSON.stringify({ version: '1', tsSnapshot: 1, portfolio: { BTCUSDT: { qty: 1, avgPrice: 100, realizedPnl: 0, updatedAtTs: 1 } } })
    );
    await fs.writeFile(
      newer,
      JSON.stringify({ version: '1', tsSnapshot: 2, portfolio: { BTCUSDT: { qty: 2, avgPrice: 200, realizedPnl: 10, updatedAtTs: 2 } } })
    );

    const coordinator = new SnapshotCoordinator(bus, { portfolio }, { baseDir: tmpDir, runId: 'test' });
    coordinator.start();

    const waitLoaded = waitForEvent(bus, 'state:recovery_loaded');
    bus.publish('state:recovery_requested', { meta: createMeta('system') });
    const loaded = (await waitLoaded) as RecoveryLoaded;

    expect(loaded.path).toBe(newer);
    expect((portfolio as any).getState()).toMatchObject({ BTCUSDT: { qty: 2, avgPrice: 200 } });

    coordinator.stop();
  });

  it('round-trips portfolio state through snapshot and recovery', async () => {
    const tmpDir = await makeTempDir();
    const bus = createTestEventBus();
    const portfolio = new PortfolioManager(bus);
    const initialState = { ETHUSDT: { qty: 3, avgPrice: 1500, realizedPnl: 5, updatedAtTs: 123 } };
    portfolio.setState(initialState);

    const coordinator = new SnapshotCoordinator(bus, { portfolio }, { baseDir: tmpDir, runId: 'round' });
    coordinator.start();

    const targetPath = path.join(tmpDir, 'round.json');
    const waitWritten = waitForEvent(bus, 'state:snapshot_written');
    bus.publish('state:snapshot_requested', { meta: createMeta('system'), pathOverride: targetPath });
    await waitWritten;

    portfolio.setState({});
    const waitLoaded = waitForEvent(bus, 'state:recovery_loaded');
    bus.publish('state:recovery_requested', { meta: createMeta('system'), pathOverride: targetPath });
    await waitLoaded;

    expect(portfolio.getState()).toEqual(initialState);

    coordinator.stop();
  });

  it('emits state:recovery_failed when snapshot cannot be read', async () => {
    const tmpDir = await makeTempDir();
    const bus = createTestEventBus();
    const coordinator = new SnapshotCoordinator(bus, {}, { baseDir: tmpDir, runId: 'fail' });
    coordinator.start();

    const badPath = path.join(tmpDir, 'bad.json');
    await fs.writeFile(badPath, 'not-json', 'utf8');
    const waitFailed = waitForEvent(bus, 'state:recovery_failed');
    bus.publish('state:recovery_requested', { meta: createMeta('system'), pathOverride: badPath });
    const failed = (await waitFailed) as RecoveryFailed;

    expect(failed.path).toBe(badPath);
    expect(failed.reason).toBeTruthy();

    coordinator.stop();
  });
});
