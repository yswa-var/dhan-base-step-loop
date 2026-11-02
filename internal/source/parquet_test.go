package source

import (
	"context"
	"dhan-base/internal/event"
	"dhan-base/internal/sink"
	"io"
	"path/filepath"
	"testing"
	"time"
)

func TestParquetRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := sink.New(dir, time.Hour, 10, 10)
	w.Start(context.Background())
	tick := event.Tick{TsExchange: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC).UnixNano(), Symbol: "X", LTP: 100, Bid: 99, Ask: 101}
	book := event.BookSnapshot{TsExchange: tick.TsExchange + 1, Symbol: "X", Bids: []event.Level{{Price: 99, Qty: 2}}, Asks: []event.Level{{Price: 101, Qty: 3}}}
	w.Offer(tick)
	w.Offer(book)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := OpenParquet(dir, "2026-11-02", "2026-11-02", []string{"X"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []event.Event{tick, book} {
		got, err := p.Next(context.Background())
		if err != nil || got.EventTime() != want.EventTime() {
			t.Fatalf("got %v %v", got, err)
		}
	}
	if _, err := p.Next(context.Background()); err != io.EOF {
		t.Fatalf("want EOF: %v", err)
	}
	w2 := sink.New(dir, time.Hour, 10, 10)
	w2.Start(context.Background())
	w2.Offer(tick)
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "ticks", "date=*", "symbol=X", "part-*.parquet"))
	if err != nil || len(files) != 2 {
		t.Fatalf("append overwrote a part: %v %v", files, err)
	}
}
