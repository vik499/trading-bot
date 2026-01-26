import fs from 'node:fs/promises';
import path from 'node:path';
import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    inheritMeta,
    eventBus as defaultEventBus,
    type EventBus,
    type EventMeta,
    type SnapshotRequested,
    type SnapshotWritten,
    type RecoveryRequested,
    type RecoveryLoaded,
    type RecoveryFailed,
} from '../core/events/EventBus';
import { formatUtcTimestamp } from '../core/time/utc';

interface Stateful<T> {
    getState(): T;
    setState(state: T): void;
}

interface SnapshotCoordinatorDeps {
    portfolio?: Stateful<unknown>;
    risk?: Stateful<unknown>;
    strategy?: Stateful<unknown>;
    analytics?: Stateful<unknown>;
}

export interface SnapshotCoordinatorOptions {
    baseDir?: string;
    runId?: string;
}

type SnapshotVersion = '1';

interface SnapshotFile {
    version: SnapshotVersion;
    runId?: string;
    tsSnapshot: number;
    portfolio?: unknown;
    risk?: unknown;
    strategy?: unknown;
    analytics?: unknown;
}

export class SnapshotCoordinator {
    private readonly bus: EventBus;
    private readonly opts: Required<SnapshotCoordinatorOptions>;
    private readonly deps: SnapshotCoordinatorDeps;
    private started = false;
    private unsubscribeSnapshot?: () => void;
    private unsubscribeRecovery?: () => void;

    constructor(bus: EventBus = defaultEventBus, deps: SnapshotCoordinatorDeps = {}, opts: SnapshotCoordinatorOptions = {}) {
        this.bus = bus;
        this.deps = deps;
        this.opts = {
            baseDir: opts.baseDir ?? process.env.BOT_STATE_DIR ?? './data/state',
            runId: opts.runId ?? process.env.BOT_RUN_ID ?? 'default',
        };
    }

    start(): void {
        if (this.started) return;
        const onSnapshot = (req: SnapshotRequested) => void this.handleSnapshot(req);
        const onRecovery = (req: RecoveryRequested) => void this.handleRecovery(req);
        this.bus.subscribe('state:snapshot_requested', onSnapshot);
        this.bus.subscribe('state:recovery_requested', onRecovery);
        this.unsubscribeSnapshot = () => this.bus.unsubscribe('state:snapshot_requested', onSnapshot);
        this.unsubscribeRecovery = () => this.bus.unsubscribe('state:recovery_requested', onRecovery);
        this.started = true;
        logger.info(m('lifecycle', '[SnapshotCoordinator] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribeSnapshot?.();
        this.unsubscribeRecovery?.();
        this.unsubscribeSnapshot = undefined;
        this.unsubscribeRecovery = undefined;
        this.started = false;
    }

    private async handleSnapshot(req: SnapshotRequested): Promise<void> {
        const ts = req.meta.ts;
        const runId = req.runId ?? this.opts.runId;
        const baseDir = this.opts.baseDir;
        const fileName = req.pathOverride ?? this.makeFileName(baseDir, runId, ts);

        const snapshot: SnapshotFile = {
            version: '1',
            runId,
            tsSnapshot: ts,
            portfolio: this.deps.portfolio?.getState(),
            risk: this.deps.risk?.getState(),
            strategy: this.deps.strategy?.getState(),
            analytics: this.deps.analytics?.getState(),
        };

        try {
            await fs.mkdir(path.dirname(fileName), { recursive: true });
            const tmp = `${fileName}.tmp`;
            const json = JSON.stringify(snapshot);
            await fs.writeFile(tmp, json, 'utf8');
            await fs.rename(tmp, fileName);
            const written: SnapshotWritten = {
                runId,
                path: fileName,
                bytes: Buffer.byteLength(json, 'utf8'),
                tsSnapshot: ts,
                meta: this.buildMeta(req.meta),
            };
            this.bus.publish('state:snapshot_written', written);
        } catch (error) {
            const payload: RecoveryFailed = {
                runId,
                path: fileName,
                reason: (error as Error)?.message ?? 'snapshot failed',
                error,
                meta: this.buildMeta(req.meta),
            };
            this.bus.publish('state:recovery_failed', payload);
        }
    }

    private async handleRecovery(req: RecoveryRequested): Promise<void> {
        const runId = req.runId ?? this.opts.runId;
        const baseDir = this.opts.baseDir;
        try {
            const target = req.pathOverride ?? (await this.findLatestSnapshot(baseDir, runId));
            if (!target) {
                throw new Error('no snapshot found');
            }
            const raw = await fs.readFile(target, 'utf8');
            const parsed = this.parseSnapshot(raw);

            if (parsed.portfolio && this.deps.portfolio) this.deps.portfolio.setState(parsed.portfolio);
            if (parsed.risk && this.deps.risk) this.deps.risk.setState(parsed.risk);
            if (parsed.strategy && this.deps.strategy) this.deps.strategy.setState(parsed.strategy);
            if (parsed.analytics && this.deps.analytics) this.deps.analytics.setState(parsed.analytics);

            const loaded: RecoveryLoaded = {
                runId,
                path: target,
                state: { ...parsed },
                meta: this.buildMeta(req.meta),
            };
            this.bus.publish('state:recovery_loaded', loaded);
        } catch (error) {
            const failed: RecoveryFailed = {
                runId,
                path: req.pathOverride,
                reason: (error as Error)?.message ?? 'recovery failed',
                error,
                meta: this.buildMeta(req.meta),
            };
            this.bus.publish('state:recovery_failed', failed);
        }
    }

    private async findLatestSnapshot(baseDir: string, runId: string): Promise<string | undefined> {
        const dir = path.join(baseDir);
        let entries: string[] = [];
        try {
            entries = await fs.readdir(dir);
        } catch {
            return undefined;
        }
        const files = entries
            .filter((f) => f.startsWith(`snapshot-${runId}-`) && f.endsWith('.json'))
            .map((f) => path.join(dir, f))
            .sort();
        return files[files.length - 1];
    }

    private makeFileName(baseDir: string, runId: string, ts: number): string {
        const iso = formatUtcTimestamp(ts);
        return path.join(baseDir, `snapshot-${runId}-${iso}.json`);
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'state', { ts: parent.ts });
    }

    private parseSnapshot(raw: string): SnapshotFile {
        const parsed = JSON.parse(raw) as unknown;
        if (!this.isObject(parsed)) throw new Error('invalid snapshot format');
        if (parsed.version !== '1') throw new Error('unsupported snapshot version');
        if (typeof parsed.tsSnapshot !== 'number') throw new Error('tsSnapshot missing');
        if ('portfolio' in parsed && parsed.portfolio !== undefined && !this.isObject(parsed.portfolio)) throw new Error('invalid portfolio state');
        if ('risk' in parsed && parsed.risk !== undefined && !this.isObject(parsed.risk)) throw new Error('invalid risk state');
        if ('strategy' in parsed && parsed.strategy !== undefined && !this.isObject(parsed.strategy)) throw new Error('invalid strategy state');
        if ('analytics' in parsed && parsed.analytics !== undefined && !this.isObject(parsed.analytics)) throw new Error('invalid analytics state');

        return {
            version: '1',
            runId: typeof parsed.runId === 'string' ? parsed.runId : undefined,
            tsSnapshot: parsed.tsSnapshot,
            portfolio: parsed.portfolio,
            risk: parsed.risk,
            strategy: parsed.strategy,
            analytics: parsed.analytics,
        } satisfies SnapshotFile;
    }

    private isObject(value: unknown): value is Record<string, unknown> {
        return typeof value === 'object' && value !== null && !Array.isArray(value);
    }
}
