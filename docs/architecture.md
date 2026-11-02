# Pipeline architecture

## Fixed core

One process runs one engine loop. A Source emits events; the engine runs
`state, orders = Step(state, event, cfg)`; a Broker accepts orders and returns
acks/fills as events; a Sink journals events. Replay and live use the exact
same engine and pure Step. Source, Broker, and Sink are the only swapped pieces.

```text
Source (WebSocket | mock | parquet) -> Engine -> Broker (sim | live stub)
                                         |  ^           |
                                         |  +-- ack/fill
                                         v
                                    Parquet Sink
```

## Configure instruments with a single list

Mock and replay use the distinct symbols in `instruments`; the provider's
WebSocket subscription message remains a TODO. Each symbol
may run several strategy instances. Config order sets deterministic dispatch
priority. Instance IDs are unique across the process and namespace their
private state and published signals. A record-only instrument has an empty
`strategies` list.

```yaml
mode: paper
instruments:
  - symbol: BANKNIFTY-Nov2026-FUT
    exchange: NSE
    segment: futures
    security_id: "61466" # verify against the provider instrument master
    book_levels: 20       # 5, 20, or 200
    strategies:
      - id: bank_momentum
        kind: momentum_vol
        enabled: true
      - id: bank_context
        kind: book_context
        enabled: true
  - symbol: SECOND-SYMBOL
    security_id: "TODO"
    book_levels: 5
    strategies:
      - id: second_momentum
        kind: momentum_vol
        enabled: true
```

This is a design example, not a live-ready config. Startup validation rejects
duplicate symbols/IDs, unknown kinds, invalid depth, and missing provider IDs
in live mode. The `ticker_to_test.txt` data row has 14 fields against a
16-field header, so ambiguous fields must be confirmed before live use.

## One normalized book event for every feed depth

```go
type Level struct {
    Price  float64
    Qty    int64
    Orders int64 // zero when the feed does not supply it
}
type BookSnapshot struct {
    TsExchange, TsRecv int64
    Symbol string
    Bids, Asks []Level // best price first; each length <= 5, 20, or 200
}
```

`Tick` keeps the required LTP, LTQ, volume, and top bid/ask fields. A book
update may arrive without a trade tick. `Step` handles both event types.
Provider-specific pure decoders for 5/20/200 levels produce `BookSnapshot`;
they validate prices, quantities, sort order, counts, and timestamps. A source
reconstructs a full snapshot if its wire feed sends deltas. A reconnect emits
`Gap`, clears that source's book, and resets the symbol's rolling features in
Step. Parquet stores normalized events, never provider wire payloads. The sim
broker takes the opposite top-of-book at its scheduled fill time. Depth can
later support size-aware simulation without changing strategy handlers.

## Strategies and communication

Each strategy instance owns state under its ID. Its handler receives its
event, configuration, and a read-only view of shared signals. It returns new
private state, order intents, and signal publications; it performs no I/O or
wall-clock calls. To add a strategy, implement a pure handler in
`internal/strategy`, register its `kind` in validation and Step dispatch,
and add an instance to `instruments[].strategies`.

```go
type SignalKey struct { StrategyID, Symbol, Name string }
type Signal struct {
    Value float64
    EventTime, ExpiresAt int64
}
```

A BANKNIFTY strategy can publish `book_imbalance`; a NIFTY strategy reads it
by explicit publisher ID, symbol, and name. Signals live inside returned
`State`, preserving the required two-result `Step` signature. No strategy
calls another or sends it a channel message. All handlers on one event read
the same pre-event signal snapshot. Step commits their publications after
dispatch, in config order; they become visible on the **next** event. Expiry
uses event time. `Gap` invalidates signals for its symbol. This yields
deterministic communication across symbols without another goroutine.

The single-position rule applies across all instances. The first eligible
entry in config order wins; a pending order reserves the slot. Risk flattening
and exits outrank entries. Order client IDs derive deterministically from
instance ID, symbol, event sequence, and intent type.

## Ordering, risk, and storage

The engine gives accepted events a monotonically increasing sequence. Replay
uses recorded sequence order, including timers and broker feedback. Event
timestamps drive features, expiry, cooldown, staleness, and simulated latency;
late live packets do not retroactively reorder decisions. For a sim order,
the first book at or after `signal_time + sim_latency_ms` supplies the ask for
a buy or bid for a sell. Missing or stale book data cannot manufacture a fill.

The global risk gate runs in Step before entries: daily net loss, quantity,
trade frequency, stale open positions, square-off, and kill switch. The
engine converts the `KILL` file or SIGINT to a control event; Step returns
flatten orders. Live mode requires the explicit real-money flag and successful
startup position reconciliation.

```text
cmd/record, cmd/replay, cmd/live
internal/event       Tick, BookSnapshot, Gap, Timer, Fill, OrderAck
internal/source      mock, parquet, WebSocket, provider decoders
internal/sink        bounded queue, atomic parquet parts, trade journal
internal/strategy    Step, handlers, private states, shared signals, risk
internal/broker      sim, live adapter stub
internal/charges     rate-table-driven pure calculator
internal/engine      one loop and event ordering
```

Parquet stores enough book data to replay simulated fills and uses a versioned
schema. Sink overflow increments a counter and logs data loss; the engine
never blocks on the sink queue. Parts are closed to temporary files and
atomically renamed. Charge rates, Dhan 5/20/200 wire layouts, and order API
mapping remain explicit user inputs, never guessed.
