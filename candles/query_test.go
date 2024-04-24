package candles

import "testing"

func TestQueryStringBuild(t *testing.T) {
	q := Query{}
	q.Init("ETHUSDT")
	want := "symbol=ETHUSDT&interval=1s&limit=1000&startTime=1677369601000"
	var timestamp int64 = 1677369601000
	got := string(q.QueryStringBytes(timestamp))
	if got != want {
		t.Errorf("query build want: %s, got %s", want, got)
	}
}
