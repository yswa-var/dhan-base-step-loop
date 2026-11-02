package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Strategy struct {
	ID             string              `yaml:"id"`
	Kind           string              `yaml:"kind"`
	Enabled        bool                `yaml:"enabled"`
	RequireSignals []SignalRequirement `yaml:"require_signals"`
}
type SignalRequirement struct {
	StrategyID string  `yaml:"strategy_id"`
	Symbol     string  `yaml:"symbol"`
	Name       string  `yaml:"name"`
	Min        float64 `yaml:"min"`
	Max        float64 `yaml:"max"`
}
type Instrument struct {
	Symbol     string     `yaml:"symbol"`
	Exchange   string     `yaml:"exchange"`
	Segment    string     `yaml:"segment"`
	SecurityID string     `yaml:"security_id"`
	BookLevels int        `yaml:"book_levels"`
	TickSize   float64    `yaml:"tick_size"`
	Strategies []Strategy `yaml:"strategies"`
}
type Risk struct {
	MaxDailyLoss    float64 `yaml:"max_daily_loss"`
	MaxPositionQty  int64   `yaml:"max_position_qty"`
	MaxTradesPerMin int     `yaml:"max_trades_per_min"`
	MaxTradesPerDay int     `yaml:"max_trades_per_day"`
	StaleAfterS     int     `yaml:"stale_after_s"`
}
type Rates struct {
	BrokeragePerOrder float64 `yaml:"brokerage_per_order"`
	STTBuy            float64 `yaml:"stt_buy"`
	STTSell           float64 `yaml:"stt_sell"`
	CTTBuy            float64 `yaml:"ctt_buy"`
	CTTSell           float64 `yaml:"ctt_sell"`
	Exchange          float64 `yaml:"exchange"`
	SEBI              float64 `yaml:"sebi"`
	GST               float64 `yaml:"gst"`
	StampBuy          float64 `yaml:"stamp_buy"`
}
type Config struct {
	Mode              string           `yaml:"mode"`
	WSURL             string           `yaml:"ws_url"`
	Instruments       []Instrument     `yaml:"instruments"`
	Qty               int64            `yaml:"qty"`
	TrailSLRupees     float64          `yaml:"trail_sl_rupees"`
	TargetExtraRupees float64          `yaml:"target_extra_rupees"`
	VolWindowS        int              `yaml:"vol_window_s"`
	VolEntryPctile    float64          `yaml:"vol_entry_pctile"`
	BaselineSessions  int              `yaml:"baseline_sessions"`
	MomWindowS        int              `yaml:"mom_window_s"`
	MomThreshold      float64          `yaml:"mom_threshold"`
	MaxSpread         float64          `yaml:"max_spread"`
	CooldownS         int              `yaml:"cooldown_s"`
	SquareOffTime     string           `yaml:"square_off_time"`
	SimLatencyMS      int              `yaml:"sim_latency_ms"`
	SlippageTicks     int              `yaml:"slippage_ticks"`
	FlushIntervalS    int              `yaml:"flush_interval_s"`
	FlushRows         int              `yaml:"flush_rows"`
	DataDir           string           `yaml:"data_dir"`
	Risk              Risk             `yaml:"risk"`
	ChargesVerified   bool             `yaml:"charges_verified"`
	Charges           map[string]Rates `yaml:"charges"`
}

func Default() Config {
	return Config{Mode: "paper", Qty: 1, VolWindowS: 60, VolEntryPctile: 80, BaselineSessions: 5, MomWindowS: 10, SimLatencyMS: 200, FlushIntervalS: 5, FlushRows: 10000, DataDir: "data", Risk: Risk{StaleAfterS: 5}}
}

func Load(path string) (Config, error) { return LoadForMode(path, "") }
func LoadForMode(path, mode string) (Config, error) {
	c := Default()
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if err = d.Decode(&c); err != nil {
		return c, fmt.Errorf("decode config: %w", err)
	}
	if mode != "" {
		c.Mode = mode
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	switch c.Mode {
	case "record", "replay", "paper", "live":
	default:
		return fmt.Errorf("invalid mode %q", c.Mode)
	}
	if len(c.Instruments) == 0 {
		return fmt.Errorf("at least one instrument is required")
	}
	if c.Qty <= 0 || c.VolWindowS <= 0 || c.MomWindowS <= 0 || c.BaselineSessions < 3 || c.FlushIntervalS <= 0 || c.FlushRows <= 0 || c.SimLatencyMS < 0 || c.Risk.StaleAfterS <= 0 {
		return fmt.Errorf("invalid positive config value")
	}
	if c.VolEntryPctile < 0 || c.VolEntryPctile > 100 || c.MaxSpread < 0 || c.MomThreshold < 0 || c.TrailSLRupees < 0 || c.TargetExtraRupees < 0 {
		return fmt.Errorf("invalid strategy threshold")
	}
	if c.Risk.MaxPositionQty <= 0 || c.Risk.MaxTradesPerMin <= 0 || c.Risk.MaxTradesPerDay <= 0 || c.Risk.MaxDailyLoss <= 0 {
		return fmt.Errorf("risk limits must be positive")
	}
	if _, err := time.Parse("15:04", c.SquareOffTime); err != nil {
		return fmt.Errorf("square_off_time: %w", err)
	}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for _, i := range c.Instruments {
		if i.Symbol == "" || seen[i.Symbol] {
			return fmt.Errorf("empty or duplicate symbol %q", i.Symbol)
		}
		seen[i.Symbol] = true
		if i.BookLevels != 5 && i.BookLevels != 20 && i.BookLevels != 200 {
			return fmt.Errorf("%s: book_levels must be 5, 20, or 200", i.Symbol)
		}
		if c.SlippageTicks > 0 && i.TickSize <= 0 {
			return fmt.Errorf("%s: tick_size required for slippage", i.Symbol)
		}
		if c.Mode == "live" && (i.SecurityID == "" || i.SecurityID == "TODO") {
			return fmt.Errorf("%s: live security_id required", i.Symbol)
		}
		for _, s := range i.Strategies {
			if s.ID == "" || ids[s.ID] {
				return fmt.Errorf("empty or duplicate strategy id %q", s.ID)
			}
			ids[s.ID] = true
			if s.Kind != "momentum_vol" && s.Kind != "book_context" {
				return fmt.Errorf("unknown strategy kind %q", s.Kind)
			}
		}
	}
	for _, i := range c.Instruments {
		for _, s := range i.Strategies {
			for _, r := range s.RequireSignals {
				if !ids[r.StrategyID] || !seen[r.Symbol] || r.Name == "" || r.Min > r.Max {
					return fmt.Errorf("%s: invalid required signal %+v", s.ID, r)
				}
			}
		}
	}
	if c.Mode != "record" && !c.ChargesVerified {
		return fmt.Errorf("charge rates must be filled and charges_verified set before trading or replay")
	}
	if c.Mode != "record" {
		for _, i := range c.Instruments {
			r, ok := c.Charges[i.Segment]
			if !ok || r.BrokeragePerOrder+r.STTBuy+r.STTSell+r.CTTBuy+r.CTTSell+r.Exchange+r.SEBI+r.GST+r.StampBuy <= 0 {
				return fmt.Errorf("%s: charge rates are unfilled", i.Segment)
			}
		}
	}
	return nil
}
