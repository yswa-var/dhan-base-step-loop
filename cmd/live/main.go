package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"dhan-base/internal/broker"
	"dhan-base/internal/config"
	"dhan-base/internal/engine"
	"dhan-base/internal/sink"
	"dhan-base/internal/source"
	"dhan-base/internal/strategy"
)

func main() {
	path := flag.String("config", "config.yaml", "config file")
	real := flag.Bool("i-understand-this-trades-real-money", false, "allow live broker")
	mock := flag.Bool("mock", true, "paper with synthetic source")
	flag.Parse()
	c, err := config.Load(*path)
	if err != nil {
		log.Fatal(err)
	}
	if c.Mode == "live" && !*real {
		log.Fatal("live mode requires --i-understand-this-trades-real-money")
	}
	if c.Mode != "paper" && c.Mode != "live" {
		log.Fatal("cmd/live requires paper or live mode")
	}
	var b broker.Broker
	if c.Mode == "live" {
		b = broker.NewLive()
	} else {
		sim := broker.NewSim(time.Duration(c.SimLatencyMS) * time.Millisecond)
		sim.SlippageTicks = c.SlippageTicks
		for _, i := range c.Instruments {
			sim.TickSize[i.Symbol] = i.TickSize
		}
		b = sim
	}
	if _, err := b.Positions(context.Background()); err != nil {
		log.Fatal(err)
	}
	symbols := make([]string, 0, len(c.Instruments))
	for _, i := range c.Instruments {
		symbols = append(symbols, i.Symbol)
	}
	var src source.Source
	if *mock {
		src = &source.Mock{Symbols: symbols, Start: time.Now().UnixNano(), Count: 300}
	} else {
		if c.WSURL == "" {
			log.Fatal("ws_url required")
		}
		src = &source.WebSocket{URL: c.WSURL, Symbols: symbols}
	}
	base, err := engine.LoadBaseline(c.DataDir, c, time.Now().In(time.FixedZone("IST", 19800)).Format("2006-01-02"))
	if err != nil {
		log.Fatal(err)
	}
	initial := strategy.NewState()
	initial.Baseline = base
	w := sink.New(c.DataDir, time.Duration(c.FlushIntervalS)*time.Second, c.FlushRows, 10000)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	_, err = engine.Run(ctx, src, b, w, c, true, initial)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
