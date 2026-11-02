package strategy

import (
	"fmt"
	"math"
	"time"

	"dhan-base/internal/charges"
	"dhan-base/internal/config"
	"dhan-base/internal/event"
)

type SignalKey struct{ StrategyID, Symbol, Name string }
type Signal struct {
	Value                float64
	EventTime, ExpiresAt int64
}

// InstanceState is private to a configured strategy ID. A new strategy can
// store its small scalar counters here without sharing mutable maps.
type InstanceState struct{ Numbers map[string]float64 }
type bar struct {
	Sec int64
	Mid float64
}
type Features struct {
	LastTick                        int64
	Bid, Ask                        float64
	LastSec                         int64
	LastMid                         float64
	Returns                         []float64
	ReturnHead, ReturnCount         int
	Sum, SumSq                      float64
	Mids                            []bar
	MidHead, MidCount               int
	Vol, Momentum, Spread, TickRate float64
	TickSec                         int64
	TicksInSec                      int
}
type Position struct {
	Symbol, StrategyID        string
	Side                      event.Side
	Qty                       int64
	Entry, Best, Stop, Target float64
	Entered                   int64
	PendingExit               bool
}
type State struct {
	Features      map[string]Features
	Instances     map[string]InstanceState
	Signals       map[SignalKey]Signal
	Baseline      map[string]map[int][]float64 // symbol -> minute-of-day -> prior session vol
	Position      *Position
	Pending       *event.Order
	NetToday      float64
	TradesToday   int
	TradeTimes    []int64
	CooldownUntil int64
	Halted        bool
	Day           string
	Seq           uint64
}

func NewState() State {
	return State{Features: map[string]Features{}, Instances: map[string]InstanceState{}, Signals: map[SignalKey]Signal{}, Baseline: map[string]map[int][]float64{}}
}

func Step(s State, e event.Event, c config.Config) (State, []event.Order) {
	s = clone(s)
	s.Seq++
	ts := e.EventTime()
	day := time.Unix(0, ts).In(india()).Format("2006-01-02")
	if s.Day != day {
		s.Day = day
		s.NetToday = 0
		s.TradesToday = 0
		s.TradeTimes = nil
		s.Halted = false
	}
	for k, v := range s.Signals {
		if v.ExpiresAt <= ts {
			delete(s.Signals, k)
		}
	}
	visibleSignals := make(map[SignalKey]Signal, len(s.Signals))
	for k, v := range s.Signals {
		visibleSignals[k] = v
	}
	switch v := e.(type) {
	case event.Tick:
		s.Features[v.Symbol] = update(s.Features[v.Symbol], ts, v.Bid, v.Ask, true, c)
	case event.BookSnapshot:
		if len(v.Bids) > 0 && len(v.Asks) > 0 {
			s.Features[v.Symbol] = update(s.Features[v.Symbol], ts, v.Bids[0].Price, v.Asks[0].Price, false, c)
			publishBook(&s, v, c)
		}
	case event.Gap:
		delete(s.Features, v.Symbol)
		if s.Position != nil && s.Position.Symbol == v.Symbol {
			s.Halted = true
		}
		for k := range s.Signals {
			if k.Symbol == v.Symbol {
				delete(s.Signals, k)
			}
		}
	case event.Kill:
		s.Halted = true
	case event.OrderAck:
		if !v.Accepted && s.Pending != nil && s.Pending.ID == v.OrderID {
			s.Pending = nil
			if s.Position != nil {
				s.Position.PendingExit = false
			}
		}
	case event.Fill:
		applyFill(&s, v, c)
	}
	if s.Position != nil {
		p := s.Position
		f := s.Features[p.Symbol]
		if f.LastTick > 0 && ts-f.LastTick >= int64(c.Risk.StaleAfterS)*int64(time.Second) {
			s.Halted = true
		}
	}
	if s.NetToday <= -c.Risk.MaxDailyLoss {
		s.Halted = true
	}
	if s.Position != nil && s.Pending == nil {
		p := s.Position
		f := s.Features[p.Symbol]
		price := f.Bid
		if p.Side == event.Sell {
			price = f.Ask
		}
		if s.Halted || afterSquareOff(ts, c.SquareOffTime) {
			return issue(&s, c, p.StrategyID, p.Symbol, opposite(p.Side), p.Qty, ts, "flatten")
		}
		if price > 0 {
			trail := c.TrailSLRupees / float64(p.Qty)
			if p.Side == event.Buy {
				if price > p.Best {
					p.Best = price
					p.Stop = math.Max(p.Stop, price-trail)
				}
				if price <= p.Stop || price >= p.Target {
					return issue(&s, c, p.StrategyID, p.Symbol, event.Sell, p.Qty, ts, "exit")
				}
			} else {
				if price < p.Best {
					p.Best = price
					p.Stop = math.Min(p.Stop, price+trail)
				}
				if price >= p.Stop || price <= p.Target {
					return issue(&s, c, p.StrategyID, p.Symbol, event.Buy, p.Qty, ts, "exit")
				}
			}
		}
	}
	if s.Halted || s.Position != nil || s.Pending != nil || ts < s.CooldownUntil || afterSquareOff(ts, c.SquareOffTime) {
		return s, nil
	}
	if s.TradesToday >= c.Risk.MaxTradesPerDay || c.Qty > c.Risk.MaxPositionQty {
		return s, nil
	}
	cut := ts - int64(time.Minute)
	n := 0
	for _, t := range s.TradeTimes {
		if t > cut {
			n++
		}
	}
	if n >= c.Risk.MaxTradesPerMin {
		return s, nil
	}
	var sym string
	switch v := e.(type) {
	case event.Tick:
		sym = v.Symbol
	case event.BookSnapshot:
		sym = v.Symbol
	default:
		return s, nil
	}
	f := s.Features[sym]
	if f.LastTick != ts || f.Spread > c.MaxSpread || math.Abs(f.Momentum) < c.MomThreshold {
		return s, nil
	}
	if Percentile(f.Vol, s.Baseline[sym][minute(ts)]) < c.VolEntryPctile {
		return s, nil
	}
	for _, inst := range c.Instruments {
		if inst.Symbol != sym {
			continue
		}
		for _, st := range inst.Strategies {
			if !st.Enabled || st.Kind != "momentum_vol" {
				continue
			}
			if !signalsMatch(st.RequireSignals, visibleSignals) {
				continue
			}
			side := event.Buy
			if f.Momentum < 0 {
				side = event.Sell
			}
			return issue(&s, c, st.ID, sym, side, c.Qty, ts, "entry")
		}
	}
	return s, nil
}

func signalsMatch(required []config.SignalRequirement, visible map[SignalKey]Signal) bool {
	for _, r := range required {
		v, ok := visible[SignalKey{StrategyID: r.StrategyID, Symbol: r.Symbol, Name: r.Name}]
		if !ok || v.Value < r.Min || v.Value > r.Max {
			return false
		}
	}
	return true
}

func clone(s State) State {
	n := s
	n.Instances = make(map[string]InstanceState, len(s.Instances))
	for id, v := range s.Instances {
		copyState := InstanceState{Numbers: make(map[string]float64, len(v.Numbers))}
		for k, x := range v.Numbers {
			copyState.Numbers[k] = x
		}
		n.Instances[id] = copyState
	}
	n.Features = make(map[string]Features, len(s.Features))
	for k, v := range s.Features {
		v.Returns = append([]float64(nil), v.Returns...)
		v.Mids = append([]bar(nil), v.Mids...)
		n.Features[k] = v
	}
	n.Signals = make(map[SignalKey]Signal, len(s.Signals))
	for k, v := range s.Signals {
		n.Signals[k] = v
	}
	n.TradeTimes = append([]int64(nil), s.TradeTimes...)
	if s.Position != nil {
		p := *s.Position
		n.Position = &p
	}
	if s.Pending != nil {
		o := *s.Pending
		n.Pending = &o
	}
	return n
}
func update(f Features, ts int64, bid, ask float64, isTick bool, c config.Config) Features {
	if bid <= 0 || ask <= 0 || bid > ask {
		return f
	}
	mid := (bid + ask) / 2
	sec := ts / int64(time.Second)
	if isTick {
		f.LastTick = ts
	}
	f.Bid = bid
	f.Ask = ask
	f.Spread = ask - bid
	f.LastMid = mid
	if sec != f.TickSec {
		f.TickSec = sec
		f.TicksInSec = 0
	}
	if isTick {
		f.TicksInSec++
	}
	f.TickRate = float64(f.TicksInSec)
	if sec == f.LastSec {
		return f
	}
	if f.Mids == nil {
		f.Mids = make([]bar, c.MomWindowS+2)
	}
	prior := 0.0
	if f.MidCount > 0 {
		last := (f.MidHead + f.MidCount - 1) % len(f.Mids)
		prior = f.Mids[last].Mid
	}
	if prior > 0 {
		r := mid/prior - 1
		if f.Returns == nil {
			f.Returns = make([]float64, c.VolWindowS)
		}
		if f.ReturnCount == len(f.Returns) {
			old := f.Returns[f.ReturnHead]
			f.Sum -= old
			f.SumSq -= old * old
			f.Returns[f.ReturnHead] = r
			f.ReturnHead = (f.ReturnHead + 1) % len(f.Returns)
		} else {
			idx := (f.ReturnHead + f.ReturnCount) % len(f.Returns)
			f.Returns[idx] = r
			f.ReturnCount++
		}
		f.Sum += r
		f.SumSq += r * r
		if f.ReturnCount > 1 {
			mean := f.Sum / float64(f.ReturnCount)
			f.Vol = math.Sqrt(math.Max(0, f.SumSq/float64(f.ReturnCount)-mean*mean))
		}
	}
	if f.MidCount == len(f.Mids) {
		f.MidHead = (f.MidHead + 1) % len(f.Mids)
		f.MidCount--
	}
	idx := (f.MidHead + f.MidCount) % len(f.Mids)
	f.Mids[idx] = bar{sec, mid}
	f.MidCount++
	f.LastSec = sec
	for f.MidCount > 1 && f.Mids[f.MidHead].Sec < sec-int64(c.MomWindowS) {
		f.MidHead = (f.MidHead + 1) % len(f.Mids)
		f.MidCount--
	}
	base := f.Mids[f.MidHead].Mid
	if base > 0 {
		f.Momentum = mid/base - 1
	}
	return f
}
func Percentile(v float64, baseline []float64) float64 {
	if len(baseline) < 3 {
		return -1
	}
	n := 0
	for _, x := range baseline {
		if x <= v {
			n++
		}
	}
	return 100 * float64(n) / float64(len(baseline))
}
func publishBook(s *State, b event.BookSnapshot, c config.Config) {
	if len(b.Bids) == 0 || len(b.Asks) == 0 {
		return
	}
	q := float64(b.Bids[0].Qty + b.Asks[0].Qty)
	if q == 0 {
		return
	}
	for _, i := range c.Instruments {
		if i.Symbol != b.Symbol {
			continue
		}
		for _, st := range i.Strategies {
			if st.Enabled && st.Kind == "book_context" {
				s.Signals[SignalKey{StrategyID: st.ID, Symbol: b.Symbol, Name: "book_imbalance"}] = Signal{Value: float64(b.Bids[0].Qty-b.Asks[0].Qty) / q, EventTime: b.EventTime(), ExpiresAt: b.EventTime() + int64(5*time.Second)}
			}
		}
	}
}
func issue(s *State, c config.Config, id, sym string, side event.Side, qty, ts int64, reason string) (State, []event.Order) {
	o := event.Order{ID: fmt.Sprintf("%s-%s-%d-%d-%s", id, sym, ts, s.Seq, reason), StrategyID: id, Symbol: sym, Side: side, Qty: qty, SignalTime: ts, Reason: reason}
	s.Pending = &o
	return *s, []event.Order{o}
}
func applyFill(s *State, f event.Fill, c config.Config) {
	if s.Pending == nil || s.Pending.ID != f.OrderID {
		return
	}
	o := s.Pending
	s.Pending = nil
	if o.Reason == "entry" {
		target := (charges.RoundTripCharges(f.Price, f.Qty, f.Side, segment(c, f.Symbol), c.Charges) + c.TargetExtraRupees) / float64(f.Qty)
		trail := c.TrailSLRupees / float64(f.Qty)
		p := &Position{Symbol: f.Symbol, StrategyID: o.StrategyID, Side: f.Side, Qty: f.Qty, Entry: f.Price, Best: f.Price, Entered: f.Ts}
		if f.Side == event.Buy {
			p.Stop = f.Price - trail
			p.Target = f.Price + target
		} else {
			p.Stop = f.Price + trail
			p.Target = f.Price - target
		}
		s.Position = p
		s.TradesToday++
		s.TradeTimes = append(s.TradeTimes, f.Ts)
		return
	}
	if s.Position == nil {
		return
	}
	p := s.Position
	gross := (f.Price - p.Entry) * float64(p.Qty)
	if p.Side == event.Sell {
		gross = -gross
	}
	s.NetToday += gross - charges.RoundTripCharges(p.Entry, p.Qty, p.Side, segment(c, p.Symbol), c.Charges)
	s.Position = nil
	s.CooldownUntil = f.Ts + int64(c.CooldownS)*int64(time.Second)
}
func segment(c config.Config, sym string) string {
	for _, i := range c.Instruments {
		if i.Symbol == sym {
			return i.Segment
		}
	}
	return ""
}
func opposite(s event.Side) event.Side {
	if s == event.Buy {
		return event.Sell
	}
	return event.Buy
}
func india() *time.Location { l, _ := time.LoadLocation("Asia/Kolkata"); return l }
func minute(ts int64) int   { t := time.Unix(0, ts).In(india()); return t.Hour()*60 + t.Minute() }
func afterSquareOff(ts int64, s string) bool {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return true
	}
	now := time.Unix(0, ts).In(india())
	return now.Hour()*60+now.Minute() >= t.Hour()*60+t.Minute()
}
