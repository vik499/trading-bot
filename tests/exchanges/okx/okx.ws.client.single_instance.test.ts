import { describe, expect, it } from 'vitest';
import { getOkxPublicWsClient, resetOkxPublicWsClientRegistry } from '../../../src/exchange/okx/wsClient';

describe('OkxPublicWsClient registry', () => {
  it('returns a single instance per key', () => {
    resetOkxPublicWsClientRegistry();

    const first = getOkxPublicWsClient({
      url: 'wss://ws.okx.com:8443/ws/v5/public',
      streamId: 'okx.public.swap',
      marketType: 'futures',
    });
    const second = getOkxPublicWsClient({
      url: 'wss://ws.okx.com:8443/ws/v5/public',
      streamId: 'okx.public.swap',
      marketType: 'futures',
    });

    expect(first).toBe(second);
    expect(first.clientInstanceId).toBe(second.clientInstanceId);
  });
});
