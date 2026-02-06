# OKX Exchange Integration

## Streams
- Trades: WS `trades`
- Orderbook: WS `books` channels
- Klines: WS `candle*` channels (routed via business WS; see `market-data-ingestion.md`)
- Ticker: WS `tickers`
- Liquidations: WS `liquidation-orders`
- OI/Funding: REST pollers (see `src/exchange/okx/restClient.ts` and poller wiring in `src/apps/live-bot.ts`)

## Endpoints
- Public WS: `wss://ws.okx.com:8443/ws/v5/public`
- Business WS (klines): `wss://ws.okx.com:8443/ws/v5/business`

## Key Fields
- Symbol: `instId` (normalized to canonical symbol)
- Side: `side` (`buy` / `sell`)
- Price: `px` or `price`
- Size: `sz` or `size` (base units)
- Trade timestamp: `ts`
- Orderbook sequencing: `seqId` / `prevSeqId` (handled in code)

## Taker Side Rule
OKX trades include an explicit `side` string.
- If `side=buy` then aggressor is **buy**
- If `side=sell` then aggressor is **sell**

Mapping used in code (`src/exchange/okx/wsClient.ts:469-498`):
- `side = 'Buy'` when `side=buy`
- `side = 'Sell'` when `side=sell`

Signed CVD delta (canonical rule):
- `+size` for `Buy` aggressor
- `-size` for `Sell` aggressor

## Units
- Trade size `sz` is base-asset quantity
- Prices are in quote currency (e.g., USDT)

## Known Pitfalls
- Klines must be routed through business WS to avoid OKX public WS subscription errors.
- Orderbook sequencing uses `prevSeqId` and `seqId` (not strict +1); handle heartbeat updates where `prevSeqId == seqId`.
- Trades can arrive out of order; storage emits gap/out-of-order signals.
- Reconnects must resubscribe idempotently; duplicates should be ignored.

## Mapping to Our Events
- Trades: `market:trade` and `market:trade_raw`
- Orderbook: `market:orderbook_l2_snapshot`, `market:orderbook_l2_delta`, and `market:orderbook_*_raw`
- Klines: `market:kline` and `market:candle_raw`
- Prices: `market:ticker`
- Liquidations: `market:liquidation` and `market:liquidation_raw`
- OI/Funding: `market:oi`, `market:funding`
