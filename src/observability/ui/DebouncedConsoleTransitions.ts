export class DebouncedConsoleTransitions {
    private readonly debounceMs: number;
    private readonly holdDownMs: number;
    private readonly now: () => number;

    private readonly lastPrintedAt = new Map<string, number>();
    private readonly lastDegradedAt = new Map<string, number>();

    constructor(opts: { debounceMs: number; holdDownMs: number; now?: () => number }) {
        this.debounceMs = Math.max(0, Math.floor(opts.debounceMs));
        this.holdDownMs = Math.max(0, Math.floor(opts.holdDownMs));
        this.now = opts.now ?? (() => Date.now());
    }

    shouldEmitDegraded(key: string): boolean {
        const t = this.now();
        this.lastDegradedAt.set(key, t);
        const last = this.lastPrintedAt.get(key);
        if (last !== undefined && t - last < this.debounceMs) return false;
        this.lastPrintedAt.set(key, t);
        return true;
    }

    shouldEmitRecovered(key: string): boolean {
        const t = this.now();
        const degradedAt = this.lastDegradedAt.get(key);
        if (degradedAt !== undefined && t - degradedAt < this.holdDownMs) return false;
        const last = this.lastPrintedAt.get(key);
        if (last !== undefined && t - last < this.debounceMs) return false;
        this.lastPrintedAt.set(key, t);
        return true;
    }
}
