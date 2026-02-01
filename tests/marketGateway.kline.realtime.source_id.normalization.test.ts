import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { OkxPublicWsClient } from '../src/exchange/okx/wsClient';
import { createTestEventBus } from '../src/core/events/testing';
import type { KlineEvent } from '../src/core/events/EventBus';

const load = (name: string) =>
  JSON.parse(readFileSync(path.join(__dirname, 'fixtures', 'exchanges', 'okx', name), 'utf-8')) as Record<string, unknown>;

const send = (client: OkxPublicWsClient, payload: Record<string, unknown>) => {
  (client as unknown as { onMessage: (raw: string) => void }).onMessage(JSON.stringify(payload));
};

describe('MarketGateway kline realtime sourceId normalization', () => {
  it('normalizes okx.public.swap.kline streamId', () => {
    const bus = createTestEventBus();
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus, streamId: 'okx.public.swap.kline' });
    const outputs: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => outputs.push(evt));

    send(client, load('kline.json'));

    expect(outputs).toHaveLength(1);
    expect(outputs[0].streamId).toBe('okx.public.swap');
  });

  it('normalizes okx.public.spot.kline streamId', () => {
    const bus = createTestEventBus();
    const client = new OkxPublicWsClient('ws://test', { eventBus: bus, streamId: 'okx.public.spot.kline', marketType: 'spot' });
    const outputs: KlineEvent[] = [];
    bus.subscribe('market:kline', (evt) => outputs.push(evt));

    send(client, load('kline.json'));

    expect(outputs).toHaveLength(1);
    expect(outputs[0].streamId).toBe('okx.public.spot');
  });
});
