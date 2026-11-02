package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dhan-base/internal/broker"
	"dhan-base/internal/config"
	"dhan-base/internal/event"
	"dhan-base/internal/sink"
	"dhan-base/internal/source"
	"dhan-base/internal/strategy"
)

func fixture() ([]event.Event, config.Config, strategy.State) {
	start := time.Date(2026, 11, 2, 10, 0, 0, 0, time.FixedZone("IST", 19800)).UnixNano()
	var events []event.Event
	for n := 0; n < 30; n++ {
		p := 100 + float64(n)*0.2
		ts := start + int64(n)*int64(time.Second)
		events = append(events, event.Tick{TsExchange: ts, TsRecv: ts, Symbol: "X", LTP: p, Bid: p - 0.05, Ask: p + 0.05, LTQ: 1, Volume: int64(n + 1)})
	}
	c := config.Config{Qty: 1, TrailSLRupees: 2, TargetExtraRupees: 0.5, VolWindowS: 5, VolEntryPctile: 80, BaselineSessions: 3, MomWindowS: 2, MomThreshold: 0.001, MaxSpread: 1, CooldownS: 2, SquareOffTime: "15:20", SimLatencyMS: 200, Risk: config.Risk{MaxDailyLoss: 100, MaxPositionQty: 1, MaxTradesPerMin: 3, MaxTradesPerDay: 10, StaleAfterS: 5}, Instruments: []config.Instrument{{Symbol: "X", Segment: "futures", BookLevels: 5, Strategies: []config.Strategy{{ID: "x_mom", Kind: "momentum_vol", Enabled: true}}}}, Charges: map[string]config.Rates{"futures": {BrokeragePerOrder: 0.01}}}
	st := strategy.NewState()
	st.Baseline["X"] = map[int][]float64{}
	for m := 600; m <= 601; m++ {
		st.Baseline["X"][m] = []float64{0, 0, 0}
	}
	return events, c, st
}
func runFixture(t *testing.T, src source.Source, c config.Config, st strategy.State) []byte {
	t.Helper()
	b := broker.NewSim(200 * time.Millisecond)
	result, err := Run(context.Background(), src, b, nil, c, false, st)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Orders []event.Order
		Trades []Trade
	}{result.Orders, result.Trades})
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func TestDeterminismAndReplayEqualsMock(t *testing.T) {
	events, c, st := fixture()
	first := runFixture(t, &source.Slice{Events: events}, c, st)
	second := runFixture(t, &source.Slice{Events: events}, c, st)
	if !bytes.Equal(first, second) {
		t.Fatalf("non-deterministic output\n%s\n%s", first, second)
	}
	if bytes.Equal(first, []byte(`{"Orders":null,"Trades":null}`)) {
		t.Fatal("fixture produced no orders")
	}
	dir := t.TempDir()
	w := sink.New(dir, time.Hour, 100, 100)
	w.Start(context.Background())
	for _, e := range events {
		w.Offer(e)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := source.OpenParquet(dir, "2026-11-02", "2026-11-02", []string{"X"})
	if err != nil {
		t.Fatal(err)
	}
	replay := runFixture(t, p, c, st)
	if !bytes.Equal(first, replay) {
		t.Fatalf("mock and parquet differ\n%s\n%s", first, replay)
	}
}

func TestTradeJournalBytesDeterministic(t *testing.T) {
	events, c, st := fixture()
	var prior []byte
	for run := 0; run < 2; run++ {
		dir := t.TempDir()
		w := sink.New(dir, time.Hour, 1000, 1000)
		result, err := Run(context.Background(), &source.Slice{Events: events}, broker.NewSim(200*time.Millisecond), w, c, false, st)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Trades) == 0 {
			t.Fatal("fixture produced no completed trade")
		}
		files, err := filepath.Glob(filepath.Join(dir, "trades", "date=*", "part-*.parquet"))
		if err != nil || len(files) != 1 {
			t.Fatalf("journal parts: %v %v", files, err)
		}
		data, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		if run > 0 && !bytes.Equal(prior, data) {
			t.Fatal("trade journal bytes differ")
		}
		prior = data
	}
}
