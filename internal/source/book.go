package source

import (
	"dhan-base/internal/event"
	"encoding/json"
	"fmt"
)

// DecodeExampleBook normalizes a sample JSON snapshot. The Dhan binary wire
// layouts remain explicit adapters to fill in; no undocumented offsets are used.
func DecodeExampleBook(payload []byte, received int64, maxLevels int) (event.BookSnapshot, error) {
	if maxLevels != 5 && maxLevels != 20 && maxLevels != 200 {
		return event.BookSnapshot{}, fmt.Errorf("unsupported depth %d", maxLevels)
	}
	var v struct {
		TsExchange int64         `json:"ts_exchange"`
		Symbol     string        `json:"symbol"`
		Bids       []event.Level `json:"bids"`
		Asks       []event.Level `json:"asks"`
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return event.BookSnapshot{}, err
	}
	if v.Symbol == "" || v.TsExchange <= 0 || len(v.Bids) == 0 || len(v.Asks) == 0 || len(v.Bids) > maxLevels || len(v.Asks) > maxLevels {
		return event.BookSnapshot{}, fmt.Errorf("invalid book header or level count")
	}
	for side, levels := range [][]event.Level{v.Bids, v.Asks} {
		for n, l := range levels {
			if l.Price <= 0 || l.Qty < 0 || l.Orders < 0 {
				return event.BookSnapshot{}, fmt.Errorf("invalid level %d", n)
			}
			if n > 0 {
				if side == 0 && levels[n-1].Price <= l.Price {
					return event.BookSnapshot{}, fmt.Errorf("bids must descend")
				}
				if side == 1 && levels[n-1].Price >= l.Price {
					return event.BookSnapshot{}, fmt.Errorf("asks must ascend")
				}
			}
		}
	}
	if v.Bids[0].Price > v.Asks[0].Price {
		return event.BookSnapshot{}, fmt.Errorf("crossed book")
	}
	return event.BookSnapshot{TsExchange: v.TsExchange, TsRecv: received, Symbol: v.Symbol, Bids: v.Bids, Asks: v.Asks}, nil
}
