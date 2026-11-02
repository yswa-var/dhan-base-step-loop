# Event-sourced scalping pipeline

One Go process runs one engine loop and the same pure `strategy.Step` in replay,
paper, and live modes. Sources, brokers, and the parquet sink sit behind small
interfaces. [Architecture](docs/architecture.md) describes book depth and
cross-symbol signals.

## Run

`config.yaml` defaults to **paper**, but refuses replay/paper/live until you
fill the charge rates and set `charges_verified: true`. This prevents trading
with invented or zero charges. The recorder can run without rates:

```sh
go run ./cmd/record --mock --count 300
```

This writes synthetic ticks to `data/ticks/date=YYYY-MM-DD/symbol=.../`.
After filling verified charge rates and recording enough prior sessions:

```sh
go run ./cmd/replay --from 2026-11-02 --to 2026-11-02
go run ./cmd/live --mock               # paper mode
```

Replay reports orders, completed trades, gross and net P&L, win rate, maximum
drawdown, and average hold time. A same-minute baseline requires at least three
earlier recorded sessions; otherwise the strategy emits no entry orders. The
mock records only trades and top quotes. The sample JSON book adapter accepts
5, 20, or 200 levels and stores normalized snapshots; the actual provider
wire decoder remains to be supplied.

Live mode requires both `mode: live` and
`--i-understand-this-trades-real-money`. It currently exits because the broker
adapter and startup reconciliation are TODOs, so it cannot place a real order.

## Add a ticker or strategy

Add an item to `instruments` in `config.yaml` with a unique `symbol`, provider
ID, `book_levels` of 5/20/200, and a `strategies` list. Mock and replay use
the distinct symbols; the provider subscription message is a TODO. Strategy
instance IDs are unique. `momentum_vol`
produces entries and `book_context` publishes an expiring `book_imbalance`
signal keyed by instance ID and symbol. A `momentum_vol` instance can list
`require_signals` from another ticker as an entry filter; see `config.yaml`.
It reads the prior event's signal snapshot. Implement new pure handlers in
`internal/strategy` and register their kind in Step dispatch.

## Verify

```sh
go vet ./...
go test -race ./...
staticcheck ./...
```

## Assumptions and TODOs for the user

- **Charges:** Fill and verify all segment rates in `config.yaml`. Values are
  intentionally zero. The current calculator uses equal entry/exit notional
  for its entry-time target estimate and charges the completed round trip.
- **Dhan WebSocket:** Supply the actual payload schemas and subscription
  protocol for 5, 20, and 200 levels. `ParseExampleTick` and
  `DecodeExampleBook` parse a documented example JSON shape only.
- **Live broker:** Implement idempotent order submission, actual order/ack/fill
  journal events, and startup position reconciliation in `internal/broker`.
- **Instrument:** The row in `ticker_to_test.txt` has fewer fields than its
  header. Verify its security ID, segment, lot size, and tick size against the
  instrument master before using it live.
- **Session data:** Record at least three prior sessions to establish a
  same-minute volatility baseline. Confirm exchange session boundaries and
  holiday handling before relying on historical comparisons.
