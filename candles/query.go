package candles

import (
	"strconv"
)

const (
	apiUriBase     = "https://api.binance.com/api/v3/klines"
	apiQueryString = "&interval=1s&limit=1000&startTime=" // 1677369601000
)

type Query struct {
	baseLen int
	buf     []byte
}

func (q *Query) Init(symbol string) {
	q.buf = make([]byte, 0, len(apiQueryString)*2)
	q.buf = append(q.buf, "symbol="...)
	// symbol already is upper case
	q.buf = append(q.buf, symbol...)
	q.buf = append(q.buf, apiQueryString...)
	q.baseLen = len(q.buf)
}

func (q *Query) QueryStringBytes(startTime int64) []byte {
	t := strconv.FormatInt(startTime, 10)
	q.buf = append(q.buf[:q.baseLen], t...)
	return q.buf
}
