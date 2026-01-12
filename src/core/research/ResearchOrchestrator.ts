import { logger } from '../../infra/logger';
import { m } from '../logMarkers';
import { createMeta, eventBus, type BacktestCompleted, type CandidateScenarioBuilt, type NewClusterFound, type OutcomeRecorded, type ScenarioApproved, type ScenarioDisabled, type ScenarioEnabled, type ScenarioRejected } from '../events/EventBus';
import { DriftDetector } from './DriftDetector';
import { InMemoryFeatureStore, type FeatureStore } from './FeatureStore';
import { OutcomeEngine } from './OutcomeEngine';
import { ResearchRunner } from './ResearchRunner';

export interface ResearchOrchestratorOptions {
    featureStore?: FeatureStore;
    driftDetector?: DriftDetector;
    outcomeEngine?: OutcomeEngine;
    researchRunner?: ResearchRunner;
    enableFeatureRecording?: boolean;
}

/**
 * ResearchOrchestrator — композиционный слой discovery-петли.
 * Сейчас: делегирует запись/эмиссию feature vectors в FeatureStore,
 * слушает research/strategy события для наблюдения, опционально включает DriftDetector.
 * Всё событие-ориентированно, без торговли.
 */
export class ResearchOrchestrator {
    private readonly featureStore: FeatureStore;
    private readonly driftDetector?: DriftDetector;
    private readonly outcomeEngine?: OutcomeEngine;
    private readonly researchRunner?: ResearchRunner;
    private readonly enableFeatureRecording: boolean;

    private started = false;
    private unsubscribers: Array<() => void> = [];

    constructor(opts: ResearchOrchestratorOptions = {}) {
        this.featureStore = opts.featureStore ?? new InMemoryFeatureStore();
        this.driftDetector = opts.driftDetector;
        this.outcomeEngine = opts.outcomeEngine;
        this.researchRunner = opts.researchRunner;
        this.enableFeatureRecording = opts.enableFeatureRecording ?? true;
    }

    start(): void {
        if (this.started) return;

        if (this.enableFeatureRecording && typeof (this.featureStore as any).start === 'function') {
            (this.featureStore as any).start();
        }

        const backtestHandler = (evt: BacktestCompleted) => this.logEvent('backtest', evt);
        eventBus.subscribe('research:backtestCompleted', backtestHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:backtestCompleted', backtestHandler));

        const approvedHandler = (evt: ScenarioApproved) => this.logEvent('scenarioApproved', evt);
        eventBus.subscribe('research:scenarioApproved', approvedHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:scenarioApproved', approvedHandler));

        const rejectedHandler = (evt: ScenarioRejected) => this.logEvent('scenarioRejected', evt);
        eventBus.subscribe('research:scenarioRejected', rejectedHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:scenarioRejected', rejectedHandler));

        const candidateHandler = (evt: CandidateScenarioBuilt) => this.logEvent('candidate', evt);
        eventBus.subscribe('research:candidateScenarioBuilt', candidateHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:candidateScenarioBuilt', candidateHandler));

        const clusterHandler = (evt: NewClusterFound) => this.logEvent('cluster', evt);
        eventBus.subscribe('research:newClusterFound', clusterHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:newClusterFound', clusterHandler));

        const enabledHandler = (evt: ScenarioEnabled) => this.logEvent('scenarioEnabled', evt);
        eventBus.subscribe('strategy:scenarioEnabled', enabledHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('strategy:scenarioEnabled', enabledHandler));

        const disabledHandler = (evt: ScenarioDisabled) => this.logEvent('scenarioDisabled', evt);
        eventBus.subscribe('strategy:scenarioDisabled', disabledHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('strategy:scenarioDisabled', disabledHandler));

        const outcomeHandler = (evt: OutcomeRecorded) => this.logEvent('outcome', evt);
        eventBus.subscribe('research:outcomeRecorded', outcomeHandler);
        this.unsubscribers.push(() => eventBus.unsubscribe('research:outcomeRecorded', outcomeHandler));

        this.driftDetector?.start();
        this.started = true;
        logger.info(m('ok', '[Research] orchestrator started'));
    }

    stop(): void {
        this.unsubscribers.forEach((fn) => fn());
        this.unsubscribers = [];
        if (this.enableFeatureRecording && typeof (this.featureStore as any).stop === 'function') {
            (this.featureStore as any).stop();
        }
        this.driftDetector?.stop();
        this.started = false;
    }

    /** Expose helper to publish scenario outcomes from other loops if needed later. */
    recordOutcome(data: Parameters<OutcomeEngine['recordOutcome']>[0]): void {
        this.outcomeEngine?.recordOutcome(data, createMeta('research'));
    }

    getRunner(): ResearchRunner | undefined {
        return this.researchRunner;
    }

    private logEvent(kind: string, _payload: unknown): void {
        logger.debug(m('ok', `[Research] ${kind} event`));
    }
}
