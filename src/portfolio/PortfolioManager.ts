import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    createMeta,
    inheritMeta,
    asTsMs,
    type EventBus,
    type EventMeta,
    type PaperFillEvent,
    type PortfolioSnapshotEvent,
    type PortfolioUpdateEvent,
    type StrategySide,
} from '../core/events/EventBus';

interface Position {
    qty: number;
    avgPrice: number;
    realizedPnl: number;
    updatedAtTs: number;
}

export type PortfolioState = PortfolioSnapshotEvent['positions'];

export class PortfolioManager {
    private readonly bus: EventBus;
    private started = false;
    private unsubscribeFill?: () => void;
    private readonly positions = new Map<string, Position>();

    constructor(bus: EventBus = defaultEventBus) {
        this.bus = bus;
    }

    start(): void {
        if (this.started) return;
        const onFill = (fill: PaperFillEvent) => this.applyFill(fill);
        this.bus.subscribe('exec:paper_fill', onFill);
        this.unsubscribeFill = () => this.bus.unsubscribe('exec:paper_fill', onFill);
        this.started = true;
        logger.info(m('lifecycle', '[PortfolioManager] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribeFill?.();
        this.unsubscribeFill = undefined;
        this.started = false;
    }

    snapshot(ts: number): PortfolioSnapshotEvent {
        const positions: PortfolioSnapshotEvent['positions'] = {};
        for (const [symbol, pos] of this.positions.entries()) {
            positions[symbol] = { ...pos };
        }
        return {
            positions,
            ts,
            meta: createMeta('portfolio', { tsEvent: asTsMs(ts) }),
        };
    }

    getState(): PortfolioState {
        const positions: PortfolioState = {};
        for (const [symbol, pos] of this.positions.entries()) {
            positions[symbol] = { ...pos };
        }
        return positions;
    }

    setState(state: PortfolioState = {}): void {
        this.positions.clear();
        for (const [symbol, pos] of Object.entries(state ?? {})) {
            this.positions.set(symbol, { ...pos });
        }
    }

    private applyFill(fill: PaperFillEvent): void {
        const position = this.positions.get(fill.symbol) ?? {
            qty: 0,
            avgPrice: 0,
            realizedPnl: 0,
            updatedAtTs: fill.meta.ts,
        };

        const signedQty = this.signedQuantity(fill.side, fill.fillQty);
        const newQty = position.qty + signedQty;

        if (position.qty !== 0 && Math.sign(position.qty) !== Math.sign(newQty) && newQty !== 0) {
            const closingQty = -position.qty;
            position.realizedPnl += closingQty * (fill.fillPrice - position.avgPrice);
        }

        if (newQty === 0) {
            position.realizedPnl += position.qty * (fill.fillPrice - position.avgPrice);
            position.qty = 0;
            position.avgPrice = 0;
        } else {
            const notionalOld = position.avgPrice * position.qty;
            const notionalNew = fill.fillPrice * signedQty;
            position.qty = newQty;
            position.avgPrice = (notionalOld + notionalNew) / position.qty;
        }

        position.updatedAtTs = fill.meta.ts;
        this.positions.set(fill.symbol, position);

        const update: PortfolioUpdateEvent = {
            symbol: fill.symbol,
            qty: position.qty,
            avgPrice: position.avgPrice,
            realizedPnl: position.realizedPnl,
            updatedAtTs: position.updatedAtTs,
            meta: this.buildMeta(fill.meta),
        };
        this.bus.publish('portfolio:update', update);
    }

    private signedQuantity(side: StrategySide, qty: number): number {
        if (side === 'LONG') return qty;
        if (side === 'SHORT') return -qty;
        return 0;
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'portfolio', { tsEvent: parent.tsEvent ?? parent.ts });
    }
}
