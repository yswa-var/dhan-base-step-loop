package engine

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"dhan-base/internal/broker"
	"dhan-base/internal/charges"
	"dhan-base/internal/config"
	"dhan-base/internal/event"
	"dhan-base/internal/sink"
	"dhan-base/internal/source"
	"dhan-base/internal/strategy"
)

type Trade struct {
	Symbol          string
	Entered, Exited int64
	Gross, Net      float64
}
type Result struct {
	State                                   strategy.State
	Orders                                  []event.Order
	Trades                                  []Trade
	Gross, Net, MaxDrawdown, AvgHoldSeconds float64
	Wins, Events                            int
	Dropped                                 uint64
}
type readResult struct {
	e   event.Event
	err error
}
type marketBroker interface{ OnMarket(event.Event) }

// Run has one state owner. The source reader may block, but never mutates state.
// Broker feedback is processed before reading the next source event.
func Run(ctx context.Context, src source.Source, b broker.Broker, w *sink.Writer, c config.Config, realtime bool, initial ...strategy.State) (result Result, runErr error) {
	if b == nil {
		return Result{}, errors.New("broker required")
	}
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	if w != nil {
		w.Start(ctx)
		defer func() {
			if err := w.Close(); err != nil && runErr == nil {
				runErr = err
			}
			result.Dropped = w.Dropped()
		}()
	}
	st := strategy.NewState()
	if len(initial) > 0 {
		st = initial[0]
	}
	out := Result{}
	input := make(chan readResult, 1)
	go func() {
		defer close(input)
		for {
			e, err := src.Next(readCtx)
			select {
			case input <- readResult{e, err}:
			case <-readCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var timer *time.Ticker
	var ticks <-chan time.Time
	if realtime {
		timer = time.NewTicker(time.Second)
		ticks = timer.C
		defer timer.Stop()
	}
	peak := 0.0
	submitCtx := ctx
	process := func(e event.Event) error {
		if w != nil {
			w.Offer(e)
		}
		out.Events++
		if f, ok := e.(event.Fill); ok && st.Position != nil && st.Pending != nil && st.Pending.ID == f.OrderID && st.Pending.Reason != "entry" {
			p := st.Position
			gross := (f.Price - p.Entry) * float64(p.Qty)
			if p.Side == event.Sell {
				gross = -gross
			}
			seg := ""
			for _, i := range c.Instruments {
				if i.Symbol == p.Symbol {
					seg = i.Segment
				}
			}
			fee := charges.RoundTripCharges(p.Entry, p.Qty, p.Side, seg, c.Charges)
			net := gross - fee
			out.Trades = append(out.Trades, Trade{Symbol: p.Symbol, Entered: p.Entered, Exited: f.Ts, Gross: gross, Net: net})
			if w != nil {
				w.Offer(event.Trade{Symbol: p.Symbol, StrategyID: p.StrategyID, Entered: p.Entered, Exited: f.Ts, Qty: p.Qty, EntryPrice: p.Entry, ExitPrice: f.Price, Gross: gross, Charges: fee, Net: net})
			}
			out.Gross += gross
			out.Net += net
			if net > 0 {
				out.Wins++
			}
			out.AvgHoldSeconds += float64(f.Ts-p.Entered) / float64(time.Second)
			if out.Net > peak {
				peak = out.Net
			}
			if peak-out.Net > out.MaxDrawdown {
				out.MaxDrawdown = peak - out.Net
			}
		}
		next, orders := strategy.Step(st, e, c)
		st = next
		for _, o := range orders {
			out.Orders = append(out.Orders, o)
			if w != nil {
				w.Offer(o)
			}
			if err := b.Submit(submitCtx, o); err != nil {
				return err
			}
		}
		return nil
	}
	drain := func() error {
		for {
			select {
			case e := <-b.Events():
				if e == nil {
					return nil
				}
				if err := process(e); err != nil {
					return err
				}
			default:
				return nil
			}
		}
	}
	var lastTimer int64
	for {
		select {
		case r, ok := <-input:
			if !ok || errors.Is(r.err, io.EOF) {
				if err := drain(); err != nil {
					return out, err
				}
				out.State = st
				if len(out.Trades) > 0 {
					out.AvgHoldSeconds /= float64(len(out.Trades))
				}
				if w != nil {
					out.Dropped = w.Dropped()
				}
				return out, nil
			}
			if r.err != nil {
				return out, r.err
			}
			if !realtime && lastTimer != 0 {
				for t := lastTimer + int64(time.Second); t <= r.e.EventTime(); t += int64(time.Second) {
					if err := process(event.Timer{Ts: t}); err != nil {
						return out, err
					}
				}
			}
			if r.e.EventTime() > lastTimer {
				lastTimer = r.e.EventTime() / int64(time.Second) * int64(time.Second)
			}
			if err := process(r.e); err != nil {
				return out, err
			}
			if mb, ok := b.(marketBroker); ok {
				mb.OnMarket(r.e)
			}
			if err := drain(); err != nil {
				return out, err
			}
		case t := <-ticks:
			killed := false
			if _, err := os.Stat("KILL"); err == nil {
				killed = true
				if err := process(event.Kill{Ts: t.UnixNano()}); err != nil {
					return out, err
				}
			}
			if err := process(event.Timer{Ts: t.UnixNano()}); err != nil {
				return out, err
			}
			if err := drain(); err != nil {
				return out, err
			}
			if killed && st.Position == nil && st.Pending == nil {
				out.State = st
				return out, nil
			}
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			submitCtx = shutdownCtx
			if err := process(event.Kill{Ts: time.Now().UnixNano()}); err != nil {
				cancel()
				return out, err
			}
			if err := drain(); err != nil {
				cancel()
				return out, err
			}
			for st.Position != nil || st.Pending != nil {
				select {
				case e := <-b.Events():
					if e != nil {
						if err := process(e); err != nil {
							cancel()
							return out, err
						}
					}
				case <-shutdownCtx.Done():
					cancel()
					return out, errors.New("shutdown flatten was not confirmed")
				}
			}
			cancel()
			return out, ctx.Err()
		}
	}
}

func Record(ctx context.Context, src source.Source, w *sink.Writer) error {
	w.Start(ctx)
	for {
		e, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return w.Close()
		}
		if err != nil {
			_ = w.Close()
			return err
		}
		w.Offer(e)
	}
}
