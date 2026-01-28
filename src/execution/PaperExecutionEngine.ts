import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import {
    eventBus as defaultEventBus,
    inheritMeta,
    type AnalyticsFeaturesEvent,
    type EventBus,
    type EventMeta,
    type PaperFillEvent,
    type RiskApprovedIntentEvent,
} from '../core/events/EventBus';

interface PriceCacheEntry {
    price: number;
    ts: number;
}

export class PaperExecutionEngine {
    private readonly bus: EventBus;
    private started = false;
    private unsubscribeIntent?: () => void;
    private unsubscribeFeatures?: () => void;
    private readonly processed = new Set<string>();
    private readonly prices = new Map<string, PriceCacheEntry>();
    private readonly pending = new Map<string, RiskApprovedIntentEvent[]>();

    constructor(bus: EventBus = defaultEventBus) {
        this.bus = bus;
    }

    start(): void {
        if (this.started) return;
        const onApproved = (evt: RiskApprovedIntentEvent) => this.handleApproved(evt);
        const onFeatures = (evt: AnalyticsFeaturesEvent) => this.handleFeatures(evt);
        this.bus.subscribe('risk:approved_intent', onApproved);
        this.bus.subscribe('analytics:features', onFeatures);
        this.unsubscribeIntent = () => this.bus.unsubscribe('risk:approved_intent', onApproved);
        this.unsubscribeFeatures = () => this.bus.unsubscribe('analytics:features', onFeatures);
        this.started = true;
        logger.info(m('lifecycle', '[PaperExecutionEngine] started'));
    }

    stop(): void {
        if (!this.started) return;
        this.unsubscribeIntent?.();
        this.unsubscribeFeatures?.();
        this.unsubscribeIntent = undefined;
        this.unsubscribeFeatures = undefined;
        this.started = false;
    }

    private handleFeatures(evt: AnalyticsFeaturesEvent): void {
        if (!Number.isFinite(evt.lastPrice)) return;
        this.prices.set(evt.symbol, { price: evt.lastPrice, ts: evt.meta.ts });
        const queued = this.pending.get(evt.symbol);
        if (queued && queued.length) {
            this.pending.delete(evt.symbol);
            queued.forEach((intentEvt) => this.fill(intentEvt));
        }
    }

    private handleApproved(evt: RiskApprovedIntentEvent): void {
        this.fill(evt);
    }

    private fill(evt: RiskApprovedIntentEvent): void {
        const intent = evt.intent;
        if (this.processed.has(intent.intentId)) return;

        const priceEntry = this.prices.get(intent.symbol);
        if (!priceEntry) {
            const list = this.pending.get(intent.symbol) ?? [];
            list.push(evt);
            this.pending.set(intent.symbol, list);
            return;
        }

        const fillPrice = priceEntry.price;
        const fillQty = fillPrice !== 0 ? intent.targetExposureUsd / fillPrice : 0;
        const payload: PaperFillEvent = {
            intentId: intent.intentId,
            symbol: intent.symbol,
            side: intent.side,
            fillPrice,
            fillQty,
            notionalUsd: intent.targetExposureUsd,
            ts: intent.meta.ts,
            meta: this.buildMeta(intent.meta),
        };

        this.bus.publish('exec:paper_fill', payload);
        this.processed.add(intent.intentId);
    }

    private buildMeta(parent: EventMeta) {
        return inheritMeta(parent, 'trading', { tsEvent: parent.tsEvent ?? parent.ts });
    }
}
