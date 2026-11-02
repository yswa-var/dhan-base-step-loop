package engine

import (
	"context"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dhan-base/internal/config"
	"dhan-base/internal/event"
	"dhan-base/internal/source"
	"dhan-base/internal/strategy"
)

// LoadBaseline uses the last N recorded session dates strictly before `before`.
// Each minute contributes its final realized-vol observation per session.
func LoadBaseline(dir string, c config.Config, before string) (map[string]map[int][]float64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "ticks", "date=*"))
	if err != nil {
		return nil, err
	}
	var days []string
	for _, p := range paths {
		d := strings.TrimPrefix(filepath.Base(p), "date=")
		if d < before {
			days = append(days, d)
		}
	}
	sort.Strings(days)
	if len(days) > c.BaselineSessions {
		days = days[len(days)-c.BaselineSessions:]
	}
	baseline := map[string]map[int][]float64{}
	symbols := make([]string, 0, len(c.Instruments))
	for _, i := range c.Instruments {
		symbols = append(symbols, i.Symbol)
	}
	for _, day := range days {
		src, err := source.OpenParquet(dir, day, day, symbols)
		if err != nil {
			return nil, err
		}
		st := strategy.NewState()
		last := map[string]map[int]float64{}
		for {
			e, err := src.Next(context.Background())
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			st, _ = strategy.Step(st, e, c)
			var sym string
			switch v := e.(type) {
			case event.Tick:
				sym = v.Symbol
			case event.BookSnapshot:
				sym = v.Symbol
			default:
				continue
			}
			t := time.Unix(0, e.EventTime()).In(ist())
			bucket := t.Hour()*60 + t.Minute()
			if last[sym] == nil {
				last[sym] = map[int]float64{}
			}
			last[sym][bucket] = st.Features[sym].Vol
		}
		for sym, m := range last {
			if baseline[sym] == nil {
				baseline[sym] = map[int][]float64{}
			}
			for bucket, v := range m {
				baseline[sym][bucket] = append(baseline[sym][bucket], v)
			}
		}
	}
	return baseline, nil
}
func ist() *time.Location { l, _ := time.LoadLocation("Asia/Kolkata"); return l }
