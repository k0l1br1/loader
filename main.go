package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/k0l1br1/loader/candles"
)

const (
	exitOk        = 0
	exitError     = 1
	exitInterrupt = 130
)

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

	totalCandles1, err := stg.SizeCandles()
	if err != nil {
		errorPrint(errorWrap("get total candles", err))
		return exitError
	}

	intChan := make(chan os.Signal, 1)
	signal.Notify(intChan, os.Interrupt, syscall.SIGTERM)

	err = candles.Load(t, stg, intChan, opts.Symbol)
	if err != nil {
		if errors.Is(err, candles.ErrInterrupted) {
			fmt.Println("Interrupted!")
			return exitInterrupt
		}
		errorPrint(err)
		return exitError
	}

	totalCandles2, err := stg.SizeCandles()
	if err != nil {
		errorPrint(errorWrap("get total candles", err))
		return exitError
	}

	fmt.Printf("All done! Loaded %d candles, total candles %d\n", totalCandles2-totalCandles1, totalCandles2)
	return exitOk
}

func main() {
	os.Exit(run())
}
