# Bybit Exchange Integration

## Streams
- Trades: WS `publicTrade.*` or `trades.*`
- Orderbook: WS `orderbook.*`
- Klines: WS `kline.*`
- Ticker/mark/index: WS `tickers.*`
- OI/Funding: REST pollers (see `src/exchange/bybit/restClient.ts` and poller wiring in `src/apps/live-bot.ts`)

## Endpoints
- Public linear WS: `wss://stream.bybit.com/v5/public/linear`

## Key Fields
- Symbol: `s` or `symbol`
- Side: `S` or `side` (`Buy` / `Sell`)
- Price: `p` or `price`
- Size: `v` or `size` or `qty` (base units)
- Trade timestamp: `T` / `tradeTime` / `tradeTs` / `ts`
- Orderbook update id: `u` / `updateId` / `seq`
- Ticker fields: `markPrice`, `indexPrice`, `lastPrice` (from WS ticker payload)

## Taker Side Rule
Bybit trades include an explicit side string.
- If `S` / `side` is `Buy` then aggressor is **buy**
- If `S` / `side` is `Sell` then aggressor is **sell**

Mapping used in code (`src/exchange/bybit/wsClient.ts:942-976`):
- `side = 'Buy'` or `side = 'Sell'` as provided

Signed CVD delta (canonical rule):
- `+size` for `Buy` aggressor
- `-size` for `Sell` aggressor

## Units
- Trade size `v`/`qty` is base-asset quantity
- Prices are in quote currency (e.g., USDT)

## Known Pitfalls
- Multiple trade topic names; ensure both `publicTrade.*` and `trades.*` are mapped consistently.
- Orderbook updates can have gaps; resync is required when `updateId` jumps.
- Some payload fields vary across categories; normalize carefully.
- Handle reconnects with resubscribe dedup to avoid duplicated channels.
- Orderbook is snapshot + delta; ignore duplicate deltas and resync on real gaps.

## Mapping to Our Events
- Trades: `market:trade` and `market:trade_raw`
- Orderbook: `market:orderbook_l2_snapshot`, `market:orderbook_l2_delta`, and `market:orderbook_*_raw`
- Klines: `market:kline` and `market:candle_raw`
- Prices: `market:ticker`, `market:mark_price_raw`, `market:index_price_raw`
- OI/Funding: `market:oi`, `market:funding`
