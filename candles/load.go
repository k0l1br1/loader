package candles

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/valyala/fasthttp"
)

// max limit load candles 1000
type Candles [1000]Candle

var ErrInterrupted = errors.New("Interrupted")

func errorWrap(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}

func hostClient(host string) *fasthttp.HostClient {
	isTLS := true
	// single HostClient will be enough, so no need to use fasthttp.Client
	return &fasthttp.HostClient{
		Addr: fasthttp.AddMissingPort(host, isTLS),
		// increase DNS cache time to an hour instead of default minute
		Dial: (&fasthttp.TCPDialer{
			DNSCacheDuration: time.Hour,
		}).Dial,
		DisableHeaderNamesNormalizing: true,
		DisablePathNormalizing:        true,
		IsTLS:                         isTLS,
		MaxIdleConnDuration:           time.Second * 10,
		NoDefaultUserAgentHeader:      true,
	}
}

func Load(t int64, stg *Storage, intChan chan os.Signal, symbol string) error {
	q := Query{}
	q.Init(symbol)
	uri := &fasthttp.URI{}
	uri.Parse(nil, []byte(apiUriBase))
	req := &fasthttp.Request{}
	resp := &fasthttp.Response{}
	hc := hostClient(string(uri.Host()))
	defer hc.CloseIdleConnections()

	var cs Candles
	for {
		uri.SetQueryStringBytes(q.QueryStringBytes(t))
		// make an inner copy of parsed uri
		req.SetURI(uri)
		if err := hc.Do(req, resp); err != nil {
			return errorWrap("api do request", err)
		}
		n, err := parseCandles(resp.Body(), &cs)
		if err != nil {
			return errorWrap("parse candles", err)
		}
		if err = stg.Save(cs[:n]); err != nil {
			return errorWrap("save candles", err)
		}

		// conver the time of the last candle seconds to milli
		t = SecToMilli(cs[n-1].CTime)

		if len(cs) > n {
			// all done
			return nil
		}
		select {
		case <-intChan:
			return ErrInterrupted
		default:
			// meaning that the selects never block
		}
	}
}
