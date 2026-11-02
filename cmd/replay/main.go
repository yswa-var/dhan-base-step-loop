package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	from := flag.String("from", "", "start date YYYY-MM-DD")
	to := flag.String("to", "", "end date YYYY-MM-DD")
	flag.Parse()
	c, err := config.LoadForMode(*path, "replay")
	if err != nil {
		log.Fatal(err)
	}
	if *from == "" || *to == "" {
		log.Fatal("--from and --to required")
	}
	symbols := make([]string, 0, len(c.Instruments))
	for _, i := range c.Instruments {
		symbols = append(symbols, i.Symbol)
	}
	src, err := source.OpenParquet(c.DataDir, *from, *to, symbols)
	if err != nil {
		log.Fatal(err)
	}
	base, err := engine.LoadBaseline(c.DataDir, c, *from)
	if err != nil {
		log.Fatal(err)
	}
	initial := strategy.NewState()
	initial.Baseline = base
	sim := broker.NewSim(time.Duration(c.SimLatencyMS) * time.Millisecond)
	sim.SlippageTicks = c.SlippageTicks
	for _, i := range c.Instruments {
		sim.TickSize[i.Symbol] = i.TickSize
	}
	w := sink.New(c.DataDir, time.Duration(c.FlushIntervalS)*time.Second, c.FlushRows, 10000)
	result, err := engine.Run(context.Background(), src, sim, w, c, false, initial)
	if err != nil {
		log.Fatal(err)
	}
	win := 0.0
	if len(result.Trades) > 0 {
		win = 100 * float64(result.Wins) / float64(len(result.Trades))
	}
	fmt.Printf("events=%d orders=%d trades=%d gross=%.2f net=%.2f win_rate=%.1f%% max_drawdown=%.2f avg_hold_s=%.1f dropped=%d\n", result.Events, len(result.Orders), len(result.Trades), result.Gross, result.Net, win, result.MaxDrawdown, result.AvgHoldSeconds, result.Dropped)
}
