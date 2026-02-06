# Binance Exchange Integration

## Streams
- Trades: WS `trade` and `aggTrade` events (handled in `src/exchange/binance/wsClient.ts`)
- Orderbook: WS `depthUpdate`
- Klines: WS `kline`
- Mark/Index price: WS `markPriceUpdate`
- Ticker: WS `24hrTicker`
- Liquidations: WS `forceOrder`
- OI/Funding: REST pollers (see `src/exchange/binance/restClient.ts` and poller wiring in `src/apps/live-bot.ts`)

## Endpoints
- Futures WS: `wss://fstream.binance.com/ws`
- Spot WS: `wss://stream.binance.com:9443/ws`

## Key Fields
- Symbol: `s`
- Price: `p` (trade), `c` (ticker last), `p` (mark), `i` (index)
- Size: `q` (trade quantity, base units)
- Trade timestamp: `T` (trade time) or `E` (event time fallback)
- Orderbook update ids: `U` (first), `u` (last), `pu` (prev)

## Taker Side Rule
Field `m` means "buyer is maker".
- If `m=true` then the aggressor is **sell** (sell-initiated)
- If `m=false` then the aggressor is **buy** (buy-initiated)

Mapping used in code (`src/exchange/binance/wsClient.ts:437-463`):
- `side = 'Sell'` when `m=true`
- `side = 'Buy'` when `m=false`

Signed CVD delta (canonical rule):
- `+size` for `Buy` aggressor
- `-size` for `Sell` aggressor

## Units
- Trade size `q` is base-asset quantity
- Prices are in quote currency (e.g., USDT)

## Known Pitfalls
- Both `trade` and `aggTrade` are accepted; ensure side logic remains identical.
- `m` must be interpreted as aggressor inversion (buyer is maker => seller is aggressor).
- Depth updates can be out of order; resync logic is required (see `market-data-ingestion.md`).
- Orderbook requires snapshot + delta handoff (`U`/`u`); ignore duplicates and resync on gaps.
- Reconnect must resubscribe idempotently to avoid duplicate streams.

## Mapping to Our Events
- Trades: `market:trade` and `market:trade_raw`
- Orderbook: `market:orderbook_l2_snapshot`, `market:orderbook_l2_delta`, and `market:orderbook_*_raw`
- Klines: `market:kline` and `market:candle_raw`
- Prices: `market:ticker`, `market:mark_price_raw`, `market:index_price_raw`
- Liquidations: `market:liquidation` and `market:liquidation_raw`
- OI/Funding: `market:oi`, `market:funding`
