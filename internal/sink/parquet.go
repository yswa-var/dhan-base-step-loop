package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"dhan-base/internal/event"
	"github.com/parquet-go/parquet-go"
)

// Row is versioned. Payload is the JSON representation of the normalized event.
// The explicit time/symbol columns keep parts discoverable without decoding JSON.
type Row struct {
	Version      int32
	Kind, Symbol string
	Ts           int64
	Payload      []byte
}
type Writer struct {
	Dir      string
	Interval time.Duration
	MaxRows  int
	queue    chan event.Event
	dropped  atomic.Uint64
	done     chan error
}

func New(dir string, interval time.Duration, maxRows, capacity int) *Writer {
	return &Writer{Dir: dir, Interval: interval, MaxRows: maxRows, queue: make(chan event.Event, capacity), done: make(chan error, 1)}
}
func (w *Writer) Start(ctx context.Context) { go func() { w.done <- w.run(ctx) }() }
func (w *Writer) Offer(e event.Event) bool {
	select {
	case w.queue <- e:
		return true
	default:
		n := w.dropped.Add(1)
		log.Printf("SINK OVERFLOW: dropped event count=%d", n)
		return false
	}
}
func (w *Writer) Dropped() uint64 { return w.dropped.Load() }
func (w *Writer) Close() error    { close(w.queue); return <-w.done }

func (w *Writer) run(ctx context.Context) error {
	timer := time.NewTicker(w.Interval)
	defer timer.Stop()
	rows := map[string][]Row{}
	parts := map[string]int{}
	flush := func() error {
		for key, batch := range rows {
			if len(batch) == 0 {
				continue
			}
			dir := filepath.Join(w.Dir, key)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}
			if parts[key] == 0 {
				files, err := filepath.Glob(filepath.Join(dir, "part-*.parquet"))
				if err != nil {
					return err
				}
				for _, file := range files {
					var n int
					if _, err := fmt.Sscanf(filepath.Base(file), "part-%d.parquet", &n); err == nil && n > parts[key] {
						parts[key] = n
					}
				}
			}
			parts[key]++
			dst := filepath.Join(dir, fmt.Sprintf("part-%04d.parquet", parts[key]))
			tmp := dst + ".tmp"
			if err := parquet.WriteFile(tmp, batch); err != nil {
				return err
			}
			if err := os.Rename(tmp, dst); err != nil {
				return err
			}
			delete(rows, key)
		}
		return nil
	}
	for {
		select {
		case e, ok := <-w.queue:
			if !ok {
				return flush()
			}
			r, key, err := makeRow(e)
			if err != nil {
				return err
			}
			rows[key] = append(rows[key], r)
			if len(rows[key]) >= w.MaxRows {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done(): // The caller closes the queue after the source stops; drain it.
			ctx = context.Background()
		}
	}
}

func makeRow(e event.Event) (Row, string, error) {
	var kind, sym, root string
	switch v := e.(type) {
	case event.Tick:
		kind = "tick"
		sym = v.Symbol
		root = "ticks"
	case event.BookSnapshot:
		kind = "book"
		sym = v.Symbol
		root = "ticks"
	case event.Order:
		kind = "order"
		sym = v.Symbol
		root = "trades"
	case event.OrderAck:
		kind = "ack"
		root = "trades"
	case event.Fill:
		kind = "fill"
		sym = v.Symbol
		root = "trades"
	case event.Trade:
		kind = "trade"
		sym = v.Symbol
		root = "trades"
	case event.Timer:
		kind = "timer"
		root = "trades"
	case event.Gap:
		kind = "gap"
		sym = v.Symbol
		root = "ticks"
	case event.Kill:
		kind = "kill"
		root = "trades"
	default:
		return Row{}, "", fmt.Errorf("unsupported event %T", e)
	}
	if strings.ContainsAny(sym, "/\\") {
		return Row{}, "", fmt.Errorf("invalid symbol %q", sym)
	}
	b, err := json.Marshal(e)
	if err != nil {
		return Row{}, "", err
	}
	date := time.Unix(0, e.EventTime()).UTC().Format("2006-01-02")
	key := filepath.Join(root, "date="+date)
	if root == "ticks" {
		key = filepath.Join(key, "symbol="+sym)
	}
	return Row{Version: 1, Kind: kind, Symbol: sym, Ts: e.EventTime(), Payload: b}, key, nil
}
