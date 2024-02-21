package main

import (
	"errors"
	"testing"
)

func TestOptionsParser(t *testing.T) {
	args := []string{"loader", "--symbol", "ethusdt"}
	_, err := parseOptions(args[1:])
	if err != nil && !errors.Is(err, errReqSymbol) {
		t.Errorf("parse options error %s", err.Error())
	}

	opts, _ := parseOptions(args)
	wantSymbol := "ETHUSDT"
	if opts.Symbol != wantSymbol {
		t.Errorf("parse symbol want %s, got %s", wantSymbol, opts.Symbol)
	}

	args = append(args, "--is-new")
	_, err = parseOptions(args)
	if err == nil || !errors.Is(err, errReqStartTime) {
		t.Errorf("want error '%s', got '%s'", errReqStartTime.Error(), err.Error())
	}

	args1 := []string{"loader", "--symbol", "ethusdt", "-t", "2024-02-19 19:00:00"}
	_, err = parseOptions(args1)
	if err == nil || !errors.Is(err, errTimeWithoutNew) {
		t.Errorf("want error '%s', got '%s'", errTimeWithoutNew.Error(), err.Error())
	}

	args1 = append(args1, "--is-new")
	opts, _ = parseOptions(args1)
	if !opts.IsNew {
		t.Error("invalid parse --is-new flag")
	}

	var wantTimestamp int64 = 1708369200000
	if opts.StartTimestamp != wantTimestamp {
		t.Errorf("parse symbol want %d, got %d", wantTimestamp, opts.StartTimestamp)
	}
}
