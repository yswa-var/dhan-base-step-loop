# Task: Build an event-sourced scalping pipeline in Go

You are building a small, reliable trading pipeline in Go. Priorities, in order:
**correctness > simplicity > reliability > speed**. Do not add abstractions, frameworks, or dependencies that aren't needed. Prefer the standard library.

use skill dhanHQ when ever required and make a simple designed code base for easy way to add new strategies for a low iq human using a boilerplate code.

so he can start quickly and he rely this code base because you have coded it in simple and modular structure.

## Core architecture (do not deviate)

One process, one event loop, three swappable pieces and one pure function.

```
Source (live WS | parquet replay) ──► Engine loop ──► Broker (live | sim)
                                         │  ▲              │
                                         │  └── fills ─────┘ (come back as events)
                                         ▼
                                   Sink (parquet writer)
```

The engine runs `state, orders = Step(state, event, cfg)` for every event. **Live and backtest must run the exact same Engine and Step code.** Only Source and Broker implementations differ.

## Project layout

```
cmd/
  record/     # live Source -> Sink only (data recording, no trading)
  replay/     # parquet Source -> Engine -> SimBroker -> report
  live/       # live Source -> Engine -> SimBroker (paper) or LiveBroker
internal/
  event/      # Tick, Fill, OrderAck, Timer event types
  source/     # Source interface + ws and parquet implementations
  broker/     # Broker interface + sim and live implementations
  sink/       # parquet writers (ticks, trades)
  strategy/   # Step(), State, Config, features, risk
  charges/    # round-trip charge calculator
  engine/     # the loop wiring it together
config.yaml
```

## Interfaces

```go
type Tick struct {
    TsExchange, TsRecv int64 // unix nanos
    Symbol             string
    LTP, Bid, Ask      float64
    LTQ, Volume        int64
    BidQty, AskQty     int64
}

type Event interface{ EventTime() int64 }   // Tick, Fill, OrderAck, Timer

type Source interface {
    Next(ctx context.Context) (Event, error) // blocking; io.EOF at end of replay
}

type Broker interface {
    Submit(ctx context.Context, o Order) error // async; results return as events
    Events() <-chan Event                       // fills/acks
    Positions(ctx context.Context) ([]Position, error) // for startup reconciliation
}
```

The Engine merges Source events and Broker events into one ordered stream and feeds `Step`.

## Step: the pure function

```go
func Step(s State, e Event, cfg Config) (State, []Order)
```

Hard rules: **no I/O, no goroutines, no time.Now(), no randomness, no global state.** Time comes only from `e.EventTime()`. It must be fully deterministic: the same event sequence always yields the same orders. State is passed and returned by value or by explicit copy.

Inside Step, in this order:
1. Update features from the tick.
2. Risk gate (see below). If it blocks, return only flatten or no orders.
3. If flat: evaluate entry. If in position: update trailing stop, check stop, check target.
4. Return orders.

## Features (per symbol, rolling, O(1) update)

- Realized volatility: std dev of 1-second returns over `vol_window_s` (default 60).
- Volatility percentile: rank the current vol against a baseline of the **same time-of-day bucket** (e.g. 1-minute buckets) over the previous `baseline_sessions` (default 5) sessions. Baseline is loaded from parquet at startup and updated at the end of each session. If the baseline is insufficient (fewer than 3 sessions), do not trade.
- Momentum: rate of change of mid price over `mom_window_s` (default 10).
- Spread and tick rate.

## Strategy (all values come from config)

- **Entry**: vol percentile >= `vol_entry_pctile` (80, i.e. top 20%) AND momentum magnitude >= `mom_threshold` AND spread <= `max_spread`. Direction follows momentum sign.
- **Target**: `(round_trip_charges + target_extra_rupees) / qty` points from the entry price.
- **Trailing stop**: `trail_sl_rupees / qty` points behind the best price since entry. It only ratchets in the favorable direction.
- **Exit**: whichever hits first (stop or target), plus a forced flatten by `square_off_time`.
- Cooldown of `cooldown_s` seconds after each exit. One position at a time.

## Risk gate (non-negotiable, inside Step, no bypass)

- `max_daily_loss` (net of charges): on breach, flatten and halt for the day.
- `max_position_qty`, `max_trades_per_min`, `max_trades_per_day`.
- **Staleness watchdog**: the engine injects a `Timer` event every second. If no tick has arrived for `stale_after_s` on a symbol with an open position, flatten and halt.
- Kill switch: a file `KILL` in the working dir, or SIGINT, flattens everything and exits cleanly.

## Charges calculator

A pure function `RoundTripCharges(price, qty, side/segment) float64` covering brokerage, STT/CTT, exchange transaction charges, SEBI fee, GST, stamp duty. Make the rate table a config file, not hardcoded constants. Include table-driven unit tests. Mark the segment (equity intraday, futures, options) as a config option and leave the exact rates as clearly marked TODOs for me to fill in. **Do not invent rates.**

## SimBroker (backtest and paper trading)

- Fill at the **opposite side of the book** (buy at ask, sell at bid), never at LTP.
- Add a configurable latency delay (`sim_latency_ms`, default 200): the fill price is the book at `signal_time + latency`, so the broker must look ahead in the event stream by event time.
- Apply the charges calculator to every round trip. Record gross and net P&L.
- Optional `slippage_ticks` parameter.

## LiveBroker

Only implement the interface plus a stub. Leave the specific broker API as a clearly marked adapter I'll fill in. It must support: idempotent client order IDs (generated deterministically from the signal), startup position reconciliation, and every order, ack, and fill written to the journal (trades parquet).

## Live WebSocket Source

- Use `github.com/coder/websocket` (or gorilla/websocket).
- Parse into `Tick` with both `TsExchange` and `TsRecv`.
- Auto-reconnect with exponential backoff and jitter. On reconnect, emit a `Gap` marker event so the strategy can reset its rolling features.
- Make the message parser a separate pure function with tests using sample payloads (leave the payload schema as an adapter I will fill in). Also provide a `mock` Source that generates synthetic ticks so everything runs end-to-end without a broker.

## Sink / storage

- Library: `github.com/parquet-go/parquet-go` (pure Go, no cgo).
- Write ticks to `data/ticks/date=YYYY-MM-DD/symbol=XXX/part-NNNN.parquet`. Buffer in memory and flush a new part file every N seconds or M rows (configurable, default 5s). Write to a temp file and atomically rename, so readers never see a partial file.
- Trades journal to `data/trades/date=.../`.
- The Sink must never block the Engine: use a bounded channel, and on overflow count and log dropped events loudly (this should never happen in normal operation).
- Parquet Source: reads the part files for a date range and symbol list, merges them in timestamp order, and emits `Tick`s. Optionally a `speed` multiplier, but the default is as fast as possible, since the Step uses event time only.

## Concurrency model

Keep it minimal: goroutine for the Source reader, goroutine for the Sink writer, goroutine for Broker events, and **a single goroutine for the Engine loop** that owns all state. Communicate via channels. Everything is cancelled through one `context.Context` with graceful shutdown (flush the Sink, flatten positions if live).

## Testing requirements

- Table-driven unit tests for: the charges calculator, the trailing stop logic, vol percentile ranking, risk gate triggers, the parquet round trip.
- **Determinism test**: run the same recorded tick fixture through the Engine twice and assert byte-identical trade output.
- use ticker in `ticker_to_test.txt`
- **Replay-equals-live test**: feed the fixture via the mock live Source and via the parquet Source, and assert identical orders.
- Run `go vet`, `staticcheck`, and `go test -race ./...` clean.

## Config

A single `config.yaml` loaded at startup into a validated struct (fail fast on invalid values), containing: symbols, qty, trail_sl_rupees, target_extra_rupees, vol_window_s, vol_entry_pctile, baseline_sessions, mom_window_s, mom_threshold, max_spread, cooldown_s, risk limits, square_off_time, sim_latency_ms, flush interval, mode (record / replay / paper / live).

## Deliverables and process

1. Start with a short plan (file list, key types) and wait for no approval. Proceed.
2. Build in this order, with working, tested code at each stage:
   (a) event types + mock Source + Sink + `cmd/record`
   (b) Parquet Source + `cmd/replay` skeleton
   (c) features + Step + risk gate + charges
   (d) SimBroker + replay report (trades, gross/net P&L, win rate, max drawdown, avg hold time)
   (e) paper mode with the live Source and SimBroker
   (f) LiveBroker stub and reconciliation flow
3. Include a README with how to run each mode, and a clearly separated "assumptions and TODOs for the user" list (charges rates, broker adapter, WS payload schema).
4. Default `mode` must be `paper`. Live mode must require an explicit `--i-understand-this-trades-real-money` flag.

## Non-goals

No web UI, no database server, no Kafka or message queue, no ML, no multi-process setup, no cgo, no premature optimization.

## always remember:

Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
Flat is better than nested.
Sparse is better than dense.
Readability counts.
Special cases aren't special enough to break the rules.
Although practicality beats purity.
Errors should never pass silently.
Unless explicitly silenced.
In the face of ambiguity, refuse the temptation to guess.
There should be one-- and preferably only one --obvious way to do it.
Although that way may not be obvious at first unless you're Dutch.
Now is better than never.
Although never is often better than *right* now.
If the implementation is hard to explain, it's a bad idea.
If the implementation is easy to explain, it may be a good idea.
Namespaces are one honking great idea -- let's do more of those!
