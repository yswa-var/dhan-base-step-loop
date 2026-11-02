package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dhan-base/internal/event"
	"dhan-base/internal/sink"
	"github.com/parquet-go/parquet-go"
)

type Parquet struct {
	Events []event.Event
	index  int
}

func OpenParquet(dir, from, to string, symbols []string) (*Parquet, error) {
	allowed := map[string]bool{}
	order := map[string]int{}
	for n, s := range symbols {
		allowed[s] = true
		order[s] = n
	}
	var rows []sink.Row
	files, err := filepath.Glob(filepath.Join(dir, "ticks", "date=*", "symbol=*", "part-*.parquet"))
	if err != nil {
		return nil, err
	}
	for _, p := range files {
		parts := strings.Split(filepath.ToSlash(p), "/")
		var date, sym string
		for _, x := range parts {
			if strings.HasPrefix(x, "date=") {
				date = strings.TrimPrefix(x, "date=")
			}
			if strings.HasPrefix(x, "symbol=") {
				sym = strings.TrimPrefix(x, "symbol=")
			}
		}
		if date < from || date > to || !allowed[sym] {
			continue
		}
		r, err := parquet.ReadFile[sink.Row](p)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", p, err)
		}
		rows = append(rows, r...)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Ts == rows[j].Ts {
			return order[rows[i].Symbol] < order[rows[j].Symbol]
		}
		return rows[i].Ts < rows[j].Ts
	})
	out := &Parquet{}
	for _, r := range rows {
		var e event.Event
		switch r.Kind {
		case "tick":
			var v event.Tick
			err = json.Unmarshal(r.Payload, &v)
			e = v
		case "book":
			var v event.BookSnapshot
			err = json.Unmarshal(r.Payload, &v)
			e = v
		case "gap":
			var v event.Gap
			err = json.Unmarshal(r.Payload, &v)
			e = v
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		out.Events = append(out.Events, e)
	}
	return out, nil
}
func (p *Parquet) Next(ctx context.Context) (event.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.index >= len(p.Events) {
		return nil, io.EOF
	}
	e := p.Events[p.index]
	p.index++
	return e, nil
}
func TodayRange() (string, string) { d := time.Now().UTC().Format("2006-01-02"); return d, d }
