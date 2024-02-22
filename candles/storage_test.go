package candles

import (
	"io"
	"testing"
)

const (
	testFile = "/tmp/test-candles.bin"
)

var (
	a Candle = Candle{1, 1, 1, 1, 1}
	b Candle = Candle{2, 2, 2, 2, 2}
	c Candle = Candle{3, 3, 3, 3, 3}
)

func TestStorageSave(t *testing.T) {
	stg, err := NewFileStorage(testFile)
	if err != nil {
		t.Errorf("create new storage: %s", err.Error())
	}
	defer stg.Close()

	err = stg.Save([]Candle{a, b})
	if err != nil {
		t.Errorf("save candles: %s", err.Error())
	}

	// test save and get data immediately
	cTime, err := stg.LastCandleCloseTime()
	if err != nil {
		t.Errorf("get last candle close time: %s", err.Error())
	}

	wantCTime := SecToMilli(b.CTime)
	if cTime != wantCTime {
		t.Errorf("candle close time: want %d, got %d", wantCTime, cTime)
	}
}

func TestStorageAppend(t *testing.T) {
	stg, err := FileStorage(testFile)
	if err != nil {
		t.Errorf("open existing storage: %s", err.Error())
	}
	defer stg.Close()

	err = stg.Save([]Candle{c})
	if err != nil {
		t.Errorf("append candles: %s", err.Error())
	}

	// test save and get data immediately
	cTime, err := stg.FirstCandleCloseTime()
	if err != nil {
		t.Errorf("get first candle close time: %s", err.Error())
	}

	wantCTime := SecToMilli(a.CTime)
	if cTime != wantCTime {
		t.Errorf("first candle close time: want %d, got %d", wantCTime, cTime)
	}

	// test save and get data immediately
	cTime, err = stg.LastCandleCloseTime()
	if err != nil {
		t.Errorf("get appended candle close time: %s", err.Error())
	}

	wantCTime = SecToMilli(c.CTime)
	if cTime != wantCTime {
		t.Errorf("appended candle close time: want %d, got %d", wantCTime, cTime)
	}
}

func TestStorageLoad(t *testing.T) {
	stg, err := FileStorage(testFile)
	if err != nil {
		t.Errorf("open existing storage: %s", err.Error())
	}
	defer stg.Close()

	cs := make([]Candle, 2)
	n, err := stg.Read(cs)
	if err != nil && err != io.EOF {
		t.Errorf("read candles file: %s", err.Error())
	}
	if n != len(cs) {
		t.Errorf("reading candles n: want %d, got %d", len(cs), n)
	}
	if a != cs[0] {
		t.Errorf("candles not equal: want %#v, got %#v", a, cs[0])
	}
	if b != cs[1] {
		t.Errorf("candles not equal: want %#v, got %#v", b, cs[1])
	}

	n, err = stg.Read(cs)
	if err != nil && err != io.EOF {
		t.Errorf("second read candles file: %s", err.Error())
	}
	if n != len(cs) && err != io.EOF {
		t.Errorf("second reading candles n: want %d, got %d", len(cs), n)
	}
	if c != cs[0] {
		t.Errorf("candles not equal: want %#v, got %#v", c, cs[0])
	}
}
