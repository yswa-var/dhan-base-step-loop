package event

// All timestamps are Unix nanoseconds. Exchange time drives strategy decisions.
type Event interface{ EventTime() int64 }

type Level struct {
	Price  float64 `json:"price"`
	Qty    int64   `json:"qty"`
	Orders int64   `json:"orders"`
}

type Tick struct {
	TsExchange, TsRecv int64
	Symbol             string
	LTP, Bid, Ask      float64
	LTQ, Volume        int64
	BidQty, AskQty     int64
}

func (t Tick) EventTime() int64 { return t.TsExchange }

type BookSnapshot struct {
	TsExchange, TsRecv int64
	Symbol             string
	Bids, Asks         []Level
}

func (b BookSnapshot) EventTime() int64 { return b.TsExchange }

type Gap struct {
	Ts     int64
	Symbol string
}

func (g Gap) EventTime() int64 { return g.Ts }

type Timer struct{ Ts int64 }

func (t Timer) EventTime() int64 { return t.Ts }

type Kill struct{ Ts int64 }

func (k Kill) EventTime() int64 { return k.Ts }

type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

type Order struct {
	ID, StrategyID, Symbol string
	Side                   Side
	Qty                    int64
	SignalTime             int64
	Reason                 string
}

func (o Order) EventTime() int64 { return o.SignalTime }

type OrderAck struct {
	Ts       int64
	OrderID  string
	Accepted bool
	Reason   string
}

func (a OrderAck) EventTime() int64 { return a.Ts }

type Fill struct {
	Ts              int64
	OrderID, Symbol string
	Side            Side
	Qty             int64
	Price           float64
}

func (f Fill) EventTime() int64 { return f.Ts }

type Position struct {
	Symbol   string
	Qty      int64
	AvgPrice float64
}

type Trade struct {
	Symbol, StrategyID                         string
	Entered, Exited                            int64
	Qty                                        int64
	EntryPrice, ExitPrice, Gross, Charges, Net float64
}

func (t Trade) EventTime() int64 { return t.Exited }
