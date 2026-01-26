export class FakeClock {
  private current: number;
  private readonly step: number;

  constructor(start = 1_700_000_000_000, step = 1000) {
    this.current = start;
    this.step = step;
  }

  next(stepMs?: number): number {
    const value = this.current;
    this.current += stepMs ?? this.step;
    return value;
  }

  now(): number {
    return this.current;
  }

  advance(deltaMs: number): number {
    this.current += deltaMs;
    return this.current;
  }
}

export const createFakeClock = (start?: number, step?: number): FakeClock => new FakeClock(start, step);
