import { describe, expect, it, afterEach } from 'vitest';
import { eventBus, type MarketConnectRequest, type MarketSubscribeRequest } from '../src/core/events/EventBus';
import { Orchestrator } from '../src/control/orchestrator';

const envSnapshot = { ...process.env };

afterEach(() => {
  process.env = { ...envSnapshot };
});

describe('Orchestrator respects BOT_TARGET_MARKET_TYPE', () => {
  it('does not emit spot targets when target=futures', () => {
    process.env.BOT_TARGET_MARKET_TYPE = 'futures';
    process.env.BOT_SPOT_ENABLED = '1';
    process.env.BOT_SYMBOLS = 'BTCUSDT';

    const connects: MarketConnectRequest[] = [];
    const subscribes: MarketSubscribeRequest[] = [];

    const onConnect = (evt: MarketConnectRequest) => connects.push(evt);
    const onSubscribe = (evt: MarketSubscribeRequest) => subscribes.push(evt);

    eventBus.subscribe('market:connect', onConnect);
    eventBus.subscribe('market:subscribe', onSubscribe);

    const orchestrator = new Orchestrator();
    orchestrator.start();
    orchestrator.stop();

    eventBus.unsubscribe('market:connect', onConnect);
    eventBus.unsubscribe('market:subscribe', onSubscribe);

    expect(connects.length).toBeGreaterThan(0);
    expect(subscribes.length).toBeGreaterThan(0);
    expect(connects.some((evt) => evt.marketType === 'spot')).toBe(false);
    expect(subscribes.some((evt) => evt.marketType === 'spot')).toBe(false);
  });
});
