export interface HMMState {
    id: string;
    name: string;
}

export interface HMMTransition {
    from: string;
    to: string;
    probability: number;
}

export interface HMMEmission {
    stateId: string;
    mean: number;
    variance: number;
}

export interface HMMStateModel {
    states: HMMState[];
    transitions: HMMTransition[];
    emissions: HMMEmission[];
    trainedAtTs: number;
    windowSize: number;
    version: string;
}
