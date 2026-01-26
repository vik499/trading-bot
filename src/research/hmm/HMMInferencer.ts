import type { HMMStateModel } from './HMMStateModel';

export interface HMMInferenceInput {
    observations: Array<{ ts: number; value: number }>;
}

export interface HMMInferenceResult {
    stateProbabilities: Record<string, number>;
    mostLikelyState?: string;
    ts: number;
}

export class HMMInferencer {
    infer(model: HMMStateModel, input: HMMInferenceInput): HMMInferenceResult {
        const last = input.observations.length ? input.observations[input.observations.length - 1] : undefined;
        const baseProb = model.states.length ? 1 / model.states.length : 0;
        const probs: Record<string, number> = {};
        for (const state of model.states) {
            probs[state.id] = baseProb;
        }
        return {
            stateProbabilities: probs,
            mostLikelyState: model.states[0]?.id,
            ts: last?.ts ?? model.trainedAtTs,
        };
    }
}
