package main

import (
	"errors"
	"os"
	"strings"
	"time"
)

const usage = `usage: loader -s <symbol> [options]
    -s, --symbol        The pair for which need to load the prices data
    -n, --is-new        The flag to init new instance for a symbol    
    -t, --start-time    Date UTC from which to start downloading
                        (format like 2024-02-19 03:37:05)
`

var (
	errReqSymbol    = errors.New("symbol is required")
	errReqStartTime = errors.New("start-time is required for a new instance")
	// it is not clear what the user wanted, or start a new download
	// or continue saved
	errTimeWithoutNew = errors.New("start-time is required only for a new instance")
)

func help() {
	os.Stdout.WriteString(usage)
	os.Exit(exitOk)
}

type options struct {
	IsNew          bool
	Symbol         string
	StartTimestamp int64
}

func convertTimeToTimestamp(date string) (int64, error) {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, date)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func validateOptions(opts *options) error {
	if opts.Symbol == "" {
		return errReqSymbol
	}
	if opts.StartTimestamp == 0 && opts.IsNew {
		return errReqStartTime
	}
	if opts.StartTimestamp != 0 && !opts.IsNew {
		return errTimeWithoutNew
	}
	return nil
}

func parseOptions(args []string) (*options, error) {
	if len(args) < 2 {
		help()
	}
	opts := &options{}

	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "-h", "--help":
			help()
		case "-n", "--is-new":
			opts.IsNew = true
		case "-s", "--symbol":
			j := i + 1
			// during validation there will be a check for an empty string
			// it is not necessary to check it here
			if len(args) > j && !strings.HasPrefix(args[j], "-") {
				opts.Symbol = strings.ToUpper(args[j])
				i++
			}
		case "-t", "--start-time":
			j := i + 1
			if len(args) > j && !strings.HasPrefix(args[j], "-") {
				t, err := convertTimeToTimestamp(args[j])
				if err != nil {
					return nil, errorWrap("parse options start time", err)
				}
				opts.StartTimestamp = t
				i++
			}
		}
	}

	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	return opts, nil
}
