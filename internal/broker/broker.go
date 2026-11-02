package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dhan-base/internal/event"
)

type Broker interface {
	Submit(context.Context, event.Order) error
	Events() <-chan event.Event
	Positions(context.Context) ([]event.Position, error)
}

type pending struct {
	order event.Order
	due   int64
}
type Sim struct {
	Latency       time.Duration
	SlippageTicks int
	TickSize      map[string]float64
	pending       []pending
	events        chan event.Event
	positions     map[string]event.Position
}

func NewSim(latency time.Duration) *Sim {
	return &Sim{Latency: latency, events: make(chan event.Event, 1024), positions: map[string]event.Position{}, TickSize: map[string]float64{}}
}
func (s *Sim) Submit(ctx context.Context, o event.Order) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.Qty <= 0 {
		return errors.New("order quantity must be positive")
	}
	s.pending = append(s.pending, pending{o, o.SignalTime + int64(s.Latency)})
	s.events <- event.OrderAck{Ts: o.SignalTime, OrderID: o.ID, Accepted: true}
	return nil
}
func (s *Sim) Events() <-chan event.Event { return s.events }
func (s *Sim) Positions(context.Context) ([]event.Position, error) {
	p := make([]event.Position, 0, len(s.positions))
	for _, v := range s.positions {
		p = append(p, v)
	}
	return p, nil
}

// OnMarket is called only by the engine loop. It fills at the first eligible
// book after latency, never from LTP or a stale quote.
func (s *Sim) OnMarket(e event.Event) {
	var sym string
	var bid, ask float64
	switch v := e.(type) {
	case event.Tick:
		sym = v.Symbol
		bid = v.Bid
		ask = v.Ask
	case event.BookSnapshot:
		sym = v.Symbol
		if len(v.Bids) > 0 {
			bid = v.Bids[0].Price
		}
		if len(v.Asks) > 0 {
			ask = v.Asks[0].Price
		}
	default:
		return
	}
	if bid <= 0 || ask <= 0 || bid > ask {
		return
	}
	keep := s.pending[:0]
	for _, p := range s.pending {
		if p.order.Symbol != sym || e.EventTime() < p.due {
			keep = append(keep, p)
			continue
		}
		price := ask
		if p.order.Side == event.Sell {
			price = bid
		}
		slip := float64(s.SlippageTicks) * s.TickSize[sym]
		if p.order.Side == event.Buy {
			price += slip
		} else {
			price -= slip
		}
		s.events <- event.Fill{Ts: e.EventTime(), OrderID: p.order.ID, Symbol: sym, Side: p.order.Side, Qty: p.order.Qty, Price: price}
		pos := s.positions[sym]
		if p.order.Side == event.Buy {
			pos.Qty += p.order.Qty
		} else {
			pos.Qty -= p.order.Qty
		}
		pos.Symbol = sym
		pos.AvgPrice = price
		s.positions[sym] = pos
	}
	s.pending = keep
}

// Live is intentionally unusable until the order/position API is supplied.
// The adapter must use Order.ID as its idempotent client order ID and journal
// submit, ack, and fill events before exposing them to the engine.
type Live struct{ events chan event.Event }

func NewLive() *Live { return &Live{events: make(chan event.Event)} }
func (*Live) Submit(context.Context, event.Order) error {
	return errors.New("TODO: Dhan live order adapter is not configured")
}
func (l *Live) Events() <-chan event.Event { return l.events }
func (*Live) Positions(context.Context) ([]event.Position, error) {
	return nil, fmt.Errorf("TODO: Dhan startup position reconciliation adapter is not configured")
}
