package charges

import (
	"dhan-base/internal/config"
	"dhan-base/internal/event"
	"math"
	"testing"
)

func TestRoundTripCharges(t *testing.T) {
	tests := []struct {
		name  string
		rates config.Rates
		want  float64
	}{
		{"brokerage", config.Rates{BrokeragePerOrder: 10}, 20},
		{"transaction", config.Rates{Exchange: 0.001, SEBI: 0.0001}, 2.2},
		{"taxes", config.Rates{STTBuy: 0.001, STTSell: 0.002, CTTBuy: 0.003, CTTSell: 0.004}, 10},
		{"gst and stamp", config.Rates{BrokeragePerOrder: 10, GST: 0.18, StampBuy: 0.001}, 24.6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoundTripCharges(100, 10, event.Buy, "futures", map[string]config.Rates{"futures": tt.rates})
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("got %g want %g", got, tt.want)
			}
		})
	}
}
