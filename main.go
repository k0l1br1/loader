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

// max limit load candles 1000
type Candles [1000]candles.Candle

func datePrint(t int64) {
	dt := time.UnixMilli(t)
	if t == 0 {
		os.Stdout.WriteString("no date\n")
		return
	}
	os.Stdout.WriteString(dt.UTC().Format("2006-01-02 15:04:05") + "\n")
}

func errorPrint(err error) {
	os.Stderr.WriteString(err.Error() + "\n")
}

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

	if opts.ShowStart {
		t, err := stg.FirstCandleCloseTime()
		if err != nil {
			errorPrint(errorWrap("read storage first close time", err))
			return exitError
		}
		datePrint(t)
		return exitOk
	}
	if opts.ShowEnd {
		t, err := stg.LastCandleCloseTime()
		if err != nil {
			errorPrint(errorWrap("read storage last close time", err))
			return exitError
		}
		datePrint(t)
		return exitOk
	}

	t := opts.StartTimestamp
	if !opts.IsNew {
		t, err = stg.LastCandleCloseTime()
		if err != nil {
			errorPrint(errorWrap("load last close time from storage", err))
			return exitError
		}
	}

	totalCandles, err := stg.SizeCandles()
	if err != nil {
		errorPrint(errorWrap("get total candles", err))
		return exitError
	}

	q := query{}
	q.Init(opts.Symbol)
	uri := &fasthttp.URI{}
	uri.Parse(nil, []byte(apiUriBase))
	req := &fasthttp.Request{}
	resp := &fasthttp.Response{}
	hc := hostClient(string(uri.Host()))
	defer hc.CloseIdleConnections()

	intChan := make(chan os.Signal, 1)
	signal.Notify(intChan, os.Interrupt, syscall.SIGTERM)

	var cs Candles
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

		// conver the time of the last candle seconds to milli
		t = candles.SecToMilli(cs[n-1].CTime)
		totalCandles += int64(n)
		fmt.Printf("\b\rloaded  %d", totalCandles)

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
