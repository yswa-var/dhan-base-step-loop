package charges

import "dhan-base/internal/config"
import "dhan-base/internal/event"

// RoundTripCharges assumes equal entry and exit notional at price. Rates are
// fractions of notional supplied by the user; no exchange rates are embedded.
func RoundTripCharges(price float64, qty int64, entrySide event.Side, segment string, rates map[string]config.Rates) float64 {
	_ = entrySide // Equal entry/exit notional contains one buy and one sell leg.
	r := rates[segment]
	n := price * float64(qty)
	brokerage := 2 * r.BrokeragePerOrder
	transaction := 2 * n * (r.Exchange + r.SEBI)
	tax := n * (r.STTBuy + r.STTSell + r.CTTBuy + r.CTTSell)
	gst := r.GST * (brokerage + 2*n*r.Exchange)
	stamp := n * r.StampBuy
	return brokerage + transaction + tax + gst + stamp
}
