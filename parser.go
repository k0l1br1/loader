package main

import (
	"unsafe"

	"github.com/valyala/fastjson"
	"github.com/valyala/fastjson/fastfloat"
)

var parser = &fastjson.Parser{}

// b2s converts byte slice to a string without memory allocation.
// See https://groups.google.com/forum/#!msg/Golang-Nuts/ENgbUzYvCuU/90yGx7GUAgAJ .
func b2s(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func parseFloat(v *fastjson.Value) (float64, error) {
	b, err := v.StringBytes()
	if err != nil {
		return 0, err
	}
	return fastfloat.Parse(b2s(b))
}

func parseCandles(b []byte, dst *Candles) (int, error) {
	v, err := parser.ParseBytes(b)
	if err != nil {
		return 0, errorWrap("parse bytes", err)
	}
	candles, err := v.Array()
	if err != nil {
		return 0, errorWrap("parse candles", err)
	}
	var i int
	for i = 0; i < len(candles); i++ {
		c, err := candles[i].Array()
		if err != nil {
			return 0, errorWrap("parse candle", err)
		}

		t, err := c[6].Int64()
		if err != nil {
			return 0, errorWrap("parse close time", err)
		}
		// open time 1707696998000
		// close time 1707696998999
		// next open time must be 1707696999000
		// uint32(1707696998999 / 1000)         = 1707696998
		// uint32(1707696998999 / 1000) + 1     = 1707696999
		// Milli to seconds
		dst[i].CTime = uint32(t/1000) + 1

		p, err := parseFloat(c[2])
		if err != nil {
			return 0, errorWrap("parse high price", err)
		}
		dst[i].HPrice = float32(p)

		p, err = parseFloat(c[3])
		if err != nil {
			return 0, errorWrap("parse low price", err)
		}
		dst[i].LPrice = float32(p)

		p, err = parseFloat(c[4])
		if err != nil {
			return 0, errorWrap("parse close price", err)
		}
		dst[i].CPrice = float32(p)

		p, err = parseFloat(c[5])
		if err != nil {
			return 0, errorWrap("parse volume", err)
		}
		dst[i].Volume = float32(p)
	}
	return i, nil
}
