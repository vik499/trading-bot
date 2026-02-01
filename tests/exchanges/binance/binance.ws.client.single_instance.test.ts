import { beforeAll, describe, expect, it, vi } from 'vitest';

vi.mock('ws', () => {
  class MockWebSocket {
    static OPEN = 1;
    static instances = 0;
    readyState = MockWebSocket.OPEN;
    private listeners = new Map<string, (...args: any[]) => void>();

    constructor() {
      MockWebSocket.instances += 1;
      setTimeout(() => {
        const open = this.listeners.get('open');
        if (open) open();
      }, 0);
    }

    on(event: string, cb: (...args: any[]) => void): void {
      this.listeners.set(event, cb);
    }

    once(event: string, cb: (...args: any[]) => void): void {
      this.on(event, cb);
    }

    removeAllListeners(): void {
      this.listeners.clear();
    }

    send(): void {
      return;
    }

    ping(): void {
      return;
    }

    close(): void {
      const close = this.listeners.get('close');
      if (close) close(1000, Buffer.from(''));
    }
  }

  return { default: MockWebSocket };
});

let getBinancePublicWsClient: typeof import('../../../src/exchange/binance/wsClient').getBinancePublicWsClient;
let resetBinancePublicWsClientRegistry: typeof import('../../../src/exchange/binance/wsClient').resetBinancePublicWsClientRegistry;

beforeAll(async () => {
  const mod = await import('../../../src/exchange/binance/wsClient');
  getBinancePublicWsClient = mod.getBinancePublicWsClient;
  resetBinancePublicWsClientRegistry = mod.resetBinancePublicWsClientRegistry;
});

describe('BinancePublicWsClient registry', () => {
  it('returns a single instance per key', () => {
    resetBinancePublicWsClientRegistry();

    const first = getBinancePublicWsClient({
      url: 'wss://fstream.binance.com/ws',
      streamId: 'binance.usdm.public',
      marketType: 'futures',
    });
    const second = getBinancePublicWsClient({
      url: 'wss://fstream.binance.com/ws',
      streamId: 'binance.usdm.public',
      marketType: 'futures',
    });

    expect(first).toBe(second);
    expect(first.clientInstanceId).toBe(second.clientInstanceId);
  });

  it('connects only once per instance', async () => {
    resetBinancePublicWsClientRegistry();

    const client = getBinancePublicWsClient({
      url: 'wss://fstream.binance.com/ws',
      streamId: 'binance.usdm.public',
      marketType: 'futures',
    });

    await Promise.all([client.connect(), client.connect()]);

    const { default: MockWebSocket } = await import('ws');
    expect((MockWebSocket as any).instances).toBe(1);
  });
});
