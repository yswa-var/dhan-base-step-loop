package main

import (
	"context"
	"flag"
	"log"
	"time"

	"dhan-base/internal/config"
	"dhan-base/internal/engine"
	"dhan-base/internal/sink"
	"dhan-base/internal/source"
)

func main() {
	path := flag.String("config", "config.yaml", "config file")
	mock := flag.Bool("mock", true, "record synthetic ticks")
	count := flag.Int("count", 300, "mock ticks per symbol")
	flag.Parse()
	c, err := config.LoadForMode(*path, "record")
	if err != nil {
		log.Fatal(err)
	}
	symbols := make([]string, 0, len(c.Instruments))
	for _, i := range c.Instruments {
		symbols = append(symbols, i.Symbol)
	}
	var src source.Source
	if *mock {
		src = &source.Mock{Symbols: symbols, Start: time.Now().UnixNano(), Count: *count}
	} else {
		if c.WSURL == "" {
			log.Fatal("ws_url required; Dhan payload adapter is still TODO")
		}
		src = &source.WebSocket{URL: c.WSURL, Symbols: symbols}
	}
	w := sink.New(c.DataDir, time.Duration(c.FlushIntervalS)*time.Second, c.FlushRows, 10000)
	if err := engine.Record(context.Background(), src, w); err != nil {
		log.Fatal(err)
	}
}
