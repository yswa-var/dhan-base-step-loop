package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"dhan-base/internal/event"
	"github.com/gorilla/websocket"
)

// ParseExampleTick is a pure parser for an example JSON feed, not a Dhan wire
// decoder. Replace the adapter with the documented Dhan payload schema.
func ParseExampleTick(data []byte, received int64) (event.Tick, error) {
	var v struct {
		TsExchange int64   `json:"ts_exchange"`
		Symbol     string  `json:"symbol"`
		LTP        float64 `json:"ltp"`
		Bid        float64 `json:"bid"`
		Ask        float64 `json:"ask"`
		LTQ        int64   `json:"ltq"`
		Volume     int64   `json:"volume"`
		BidQty     int64   `json:"bid_qty"`
		AskQty     int64   `json:"ask_qty"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return event.Tick{}, err
	}
	if v.Symbol == "" || v.TsExchange <= 0 || v.Bid <= 0 || v.Ask <= 0 || v.Bid > v.Ask {
		return event.Tick{}, errors.New("invalid tick payload")
	}
	return event.Tick{TsExchange: v.TsExchange, TsRecv: received, Symbol: v.Symbol, LTP: v.LTP, Bid: v.Bid, Ask: v.Ask, LTQ: v.LTQ, Volume: v.Volume, BidQty: v.BidQty, AskQty: v.AskQty}, nil
}

type WebSocket struct {
	URL        string
	Symbols    []string
	conn       *websocket.Conn
	connected  bool
	backoff    time.Duration
	pendingGap []string
}

func (w *WebSocket) Next(ctx context.Context) (event.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(w.pendingGap) > 0 {
			sym := w.pendingGap[0]
			w.pendingGap = w.pendingGap[1:]
			return event.Gap{Ts: time.Now().UnixNano(), Symbol: sym}, nil
		}
		if w.conn == nil {
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, w.URL, nil)
			if err != nil {
				delay := w.backoff
				if delay == 0 {
					delay = time.Second
				}
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
				jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
				select {
				case <-time.After(delay + jitter):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				w.backoff = delay * 2
				continue
			}
			w.conn = conn
			w.backoff = time.Second
			if w.connected {
				w.pendingGap = append(w.pendingGap, w.Symbols...)
				continue
			}
			w.connected = true
		}
		_ = w.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, payload, err := w.conn.ReadMessage()
		if err != nil {
			_ = w.conn.Close()
			w.conn = nil
			continue
		}
		tick, err := ParseExampleTick(payload, time.Now().UnixNano())
		if err != nil {
			return nil, fmt.Errorf("decode websocket message: %w", err)
		}
		return tick, nil
	}
}
func (w *WebSocket) Close() error {
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}
