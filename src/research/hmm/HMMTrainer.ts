import type { HMMEmission, HMMState, HMMStateModel, HMMTransition } from './HMMStateModel';

export interface HMMTrainingSeries {
    ts: number;
    value: number;
}

export interface HMMTrainingInput {
    windowSize: number;
    observations: HMMTrainingSeries[];
    states?: HMMState[];
    version?: string;
}

export class HMMTrainer {
    train(input: HMMTrainingInput): HMMStateModel {
        const states = input.states ?? [
            { id: 'range', name: 'Range' },
            { id: 'trend', name: 'Trend' },
            { id: 'storm', name: 'Storm' },
        ];

        const transitions: HMMTransition[] = [];
        for (const from of states) {
            for (const to of states) {
                transitions.push({
                    from: from.id,
                    to: to.id,
                    probability: 1 / states.length,
                });
            }
        }

        const values = input.observations.map((o) => o.value);
        const mean = values.length ? values.reduce((sum, v) => sum + v, 0) / values.length : 0;
        const variance =
            values.length > 1
                ? values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length
                : 0;

        const emissions: HMMEmission[] = states.map((state) => ({
            stateId: state.id,
            mean,
            variance,
        }));

        const lastTs = input.observations.length ? input.observations[input.observations.length - 1].ts : 0;

        return {
            states,
            transitions,
            emissions,
            trainedAtTs: lastTs,
            windowSize: input.windowSize,
            version: input.version ?? 'hmm-v1',
        };
    }
}
