package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"loader/candles"
)

const (
	exitOk        = 0
	exitError     = 1
	exitInterrupt = 130
)

// максимальный лимит 1000 для загрузки
type Candles [1000]candles.Candle

func errorPrint(err error) {
	os.Stderr.WriteString(err.Error() + "\n")
}

func errorWrap(msg string, err error) error {
	return fmt.Errorf("%s: %w", msg, err)
}

func hostClient(host string) *fasthttp.HostClient {
	isTLS := true
	// Все равно один хост, поэтому можно не использовать fasthttp.Client
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

func run() int {
	opts, err := parseOptions(os.Args)
	if err != nil {
		errorPrint(err)
		switch err {
		case errReqSymbol, errReqStartTime, errTimeWithoutNew:
			return exitOk
		default:
			return exitError
		}
	}

	q := query{}
	q.Init(opts.Symbol)

	uri := &fasthttp.URI{}
	uri.Parse(nil, []byte(apiUriBase))

	req := &fasthttp.Request{}
	resp := &fasthttp.Response{}

	hc := hostClient(string(uri.Host()))
	defer hc.CloseIdleConnections()

	t := opts.StartTimestamp
	var cs Candles

	var stg *candles.Storage

	if opts.IsNew {
		stg, err = candles.NewDefaultStorage(opts.Symbol)
		if err != nil {
			errorPrint(errorWrap("init new storage", err))
			return exitError
		}
	} else {
		stg, err = candles.DefaultStorage(opts.Symbol)
		if err != nil {
			errorPrint(errorWrap("open storage", err))
			return exitError
		}
	}
	defer stg.Close()

	if !opts.IsNew {
		t, err = stg.LastCandleCloseTime()
		if err != nil {
			errorPrint(errorWrap("load last close time from storage", err))
			return exitError
		}
	}

	intChan := make(chan os.Signal, 1)
	signal.Notify(intChan, os.Interrupt, syscall.SIGTERM)

	totalLoaded := 0
	for {
		uri.SetQueryStringBytes(q.QueryStringBytes(t))
		// make an inner copy of parsed uri
		req.SetURI(uri)
		if err = hc.Do(req, resp); err != nil {
			errorPrint(errorWrap("api do request", err))
			return exitError
		}
		n, err := parseCandles(resp.Body(), &cs)
		if err != nil {
			errorPrint(errorWrap("parse candles", err))
			return exitError
		}
		if err = stg.Save(cs[:n]); err != nil {
			errorPrint(errorWrap("save candles", err))
			return exitError
		}

		// convert sec to milli
		t = int64(cs[n-1].CTime) * 1000
		totalLoaded += n
		fmt.Printf("\b\rloaded  %d", totalLoaded)

		if len(cs) > n {
			// all done
			fmt.Println(". All done!")
			return exitOk
		}
		select {
		case <-intChan:
			fmt.Println(". Interrupted!")
			return exitInterrupt
		default:
			// meaning that the selects never block
		}
	}
}

func main() {
	os.Exit(run())
}
