package source

import "testing"

func TestDecodeExampleBook(t *testing.T) {
	good := []byte(`{"ts_exchange":1,"symbol":"X","bids":[{"price":99,"qty":2}],"asks":[{"price":101,"qty":3}]}`)
	for _, depth := range []int{5, 20, 200} {
		b, err := DecodeExampleBook(good, 2, depth)
		if err != nil || len(b.Bids) != 1 || b.TsRecv != 2 {
			t.Fatalf("depth %d: %+v %v", depth, b, err)
		}
	}
	bad := []byte(`{"ts_exchange":1,"symbol":"X","bids":[{"price":102,"qty":2}],"asks":[{"price":101,"qty":3}]}`)
	if _, err := DecodeExampleBook(bad, 2, 5); err == nil {
		t.Fatal("crossed book accepted")
	}
}
