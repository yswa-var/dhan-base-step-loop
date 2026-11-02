package strategy

import (
	"dhan-base/internal/config"
	"dhan-base/internal/event"
	"testing"
	"time"
)

func testConfig() config.Config {
	return config.Config{Qty: 1, TrailSLRupees: 2, TargetExtraRupees: 5, VolWindowS: 5, MomWindowS: 5, VolEntryPctile: 80, MomThreshold: 0.01, MaxSpread: 1, SquareOffTime: "15:20", Risk: config.Risk{MaxDailyLoss: 100, MaxPositionQty: 1, MaxTradesPerMin: 3, MaxTradesPerDay: 10, StaleAfterS: 5}, Instruments: []config.Instrument{{Symbol: "X", Segment: "futures", BookLevels: 5, Strategies: []config.Strategy{{ID: "m", Kind: "momentum_vol", Enabled: true}}}}}
}
func at(h, m, s int) int64 {
	return time.Date(2026, 11, 2, h, m, s, 0, time.FixedZone("IST", 19800)).UnixNano()
}
func TestPercentile(t *testing.T) {
	for _, tt := range []struct {
		v    float64
		b    []float64
		want float64
	}{{4, []float64{1, 2}, -1}, {2, []float64{1, 2, 3, 4}, 50}, {5, []float64{1, 2, 3}, 100}} {
		if got := Percentile(tt.v, tt.b); got != tt.want {
			t.Fatalf("%v: %g != %g", tt.b, got, tt.want)
		}
	}
}
func TestCrossTickerSignalFilter(t *testing.T) {
	req := []config.SignalRequirement{{StrategyID: "publisher", Symbol: "BANK", Name: "book_imbalance", Min: 0.2, Max: 1}}
	key := SignalKey{StrategyID: "publisher", Symbol: "BANK", Name: "book_imbalance"}
	if signalsMatch(req, map[SignalKey]Signal{}) {
		t.Fatal("missing signal passed")
	}
	if signalsMatch(req, map[SignalKey]Signal{key: {Value: 0.1}}) {
		t.Fatal("low signal passed")
	}
	if !signalsMatch(req, map[SignalKey]Signal{key: {Value: 0.5}}) {
		t.Fatal("valid signal blocked")
	}
}

func TestSignalPassesBetweenTickers(t *testing.T) {
	c := testConfig()
	c.Instruments = append(c.Instruments, config.Instrument{Symbol: "BANK", BookLevels: 5, Strategies: []config.Strategy{{ID: "book", Kind: "book_context", Enabled: true}}})
	c.Instruments[0].Strategies[0].RequireSignals = []config.SignalRequirement{{StrategyID: "book", Symbol: "BANK", Name: "book_imbalance", Min: 0.2, Max: 1}}
	ts := at(10, 0, 3)
	s := NewState()
	s.Day = "2026-11-02"
	s.Features["X"] = Features{LastSec: ts / int64(time.Second), Momentum: 0.02, Vol: 1}
	s.Baseline["X"] = map[int][]float64{600: {0, 0, 0}}
	s, _ = Step(s, event.BookSnapshot{TsExchange: ts - 1, Symbol: "BANK", Bids: []event.Level{{Price: 99, Qty: 9}}, Asks: []event.Level{{Price: 101, Qty: 1}}}, c)
	_, orders := Step(s, event.Tick{TsExchange: ts, Symbol: "X", Bid: 100, Ask: 100.1}, c)
	if len(orders) != 1 || orders[0].StrategyID != "m" {
		t.Fatalf("cross-ticker signal did not enable entry: %+v", orders)
	}
}
func TestTrailingStopRatchets(t *testing.T) {
	c := testConfig()
	s := NewState()
	s.Position = &Position{Symbol: "X", StrategyID: "m", Side: event.Buy, Qty: 1, Entry: 100, Best: 100, Stop: 98, Target: 105, Entered: at(10, 0, 0)}
	s.Features["X"] = Features{LastTick: at(10, 0, 0), Bid: 100, Ask: 100.1}
	s, _ = Step(s, event.Tick{TsExchange: at(10, 0, 1), Symbol: "X", Bid: 103, Ask: 103.1}, c)
	if s.Position.Stop < 101 {
		t.Fatalf("stop did not ratchet: %g", s.Position.Stop)
	}
	s, _ = Step(s, event.Tick{TsExchange: at(10, 0, 2), Symbol: "X", Bid: 102, Ask: 102.1}, c)
	if s.Position.Stop < 101 {
		t.Fatal("stop moved backward")
	}
	_, orders := Step(s, event.Tick{TsExchange: at(10, 0, 3), Symbol: "X", Bid: 100.9, Ask: 101}, c)
	if len(orders) != 1 || orders[0].Side != event.Sell {
		t.Fatalf("expected stop exit: %+v", orders)
	}
}
func TestRiskStalenessAndLoss(t *testing.T) {
	c := testConfig()
	for _, tt := range []struct {
		name string
		loss float64
		tick int64
	}{{"stale", 0, at(10, 0, 0)}, {"loss", -101, at(10, 0, 5)}} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewState()
			s.Position = &Position{Symbol: "X", StrategyID: "m", Side: event.Buy, Qty: 1, Entry: 100, Best: 100, Stop: 98, Target: 105}
			s.Features["X"] = Features{LastTick: tt.tick, Bid: 100, Ask: 100.1}
			s.NetToday = tt.loss
			s.Day = "2026-11-02"
			next, o := Step(s, event.Timer{Ts: at(10, 0, 6)}, c)
			if !next.Halted || len(o) != 1 || o[0].Reason != "flatten" {
				t.Fatalf("risk failed: %+v %+v", next, o)
			}
		})
	}
}

func TestEntryRiskLimits(t *testing.T) {
	ts := at(10, 0, 3)
	for _, tt := range []struct {
		name   string
		change func(*State, *config.Config)
	}{
		{"position quantity", func(_ *State, c *config.Config) { c.Qty = 2 }},
		{"trades per minute", func(s *State, c *config.Config) { s.TradeTimes = []int64{ts - 1, ts - 2, ts - 3} }},
		{"trades per day", func(s *State, c *config.Config) { s.TradesToday = c.Risk.MaxTradesPerDay }},
		{"missing baseline", func(s *State, _ *config.Config) { delete(s.Baseline, "X") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig()
			s := NewState()
			s.Day = "2026-11-02"
			s.Features["X"] = Features{LastSec: ts / int64(time.Second), Momentum: 0.02, Vol: 1}
			s.Baseline["X"] = map[int][]float64{600: {0, 0, 0}}
			tt.change(&s, &c)
			_, orders := Step(s, event.Tick{TsExchange: ts, Symbol: "X", Bid: 100, Ask: 100.1}, c)
			if len(orders) != 0 {
				t.Fatalf("risk allowed entry: %+v", orders)
			}
		})
	}
}
