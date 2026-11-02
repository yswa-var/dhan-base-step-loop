package source

import (
	"context"
	"io"
	"math"
	"time"

	"dhan-base/internal/event"
)

type Source interface {
	Next(context.Context) (event.Event, error)
}

type Mock struct {
	Symbols      []string
	Start        int64
	Count, index int
	Step         time.Duration
}

func (m *Mock) Next(ctx context.Context) (event.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.index >= m.Count*len(m.Symbols) {
		return nil, io.EOF
	}
	n := m.index / len(m.Symbols)
	sym := m.Symbols[m.index%len(m.Symbols)]
	m.index++
	step := m.Step
	if step == 0 {
		step = time.Second
	}
	p := 100 + float64(n)*0.02 + math.Sin(float64(n)/7)*0.3
	ts := m.Start + int64(n)*int64(step)
	return event.Tick{TsExchange: ts, TsRecv: ts, Symbol: sym, LTP: p, Bid: p - 0.05, Ask: p + 0.05, LTQ: 1, Volume: int64(n + 1), BidQty: 100, AskQty: 100}, nil
}

type Slice struct {
	Events []event.Event
	index  int
}

func (s *Slice) Next(ctx context.Context) (event.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.index >= len(s.Events) {
		return nil, io.EOF
	}
	e := s.Events[s.index]
	s.index++
	return e, nil
}
