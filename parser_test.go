package main

import "testing"

func TestCandlesParser(t *testing.T) {
	jsonData := `[
        [1707696000000,"2507.22000000","2507.23000000","2507.21000000","2507.23000000","1.11450000",1707696000999,"2794.29964300",29,"0.84730000","2124.37313100","0"],
        [1707696001000,"2507.23000000","2507.31000000","2507.23000000","2507.30000000","5.10330000",1707696001999,"12795.42100000",17,"4.98940000","12509.83953000","0"],
        [1707696002000,"2507.30000000","2507.31000000","2507.30000000","2507.31000000","0.36450000",1707696002999,"913.91299400",5,"0.21440000","537.56726400","0"]
    ]`

	const prefix = "parse candles"

	var cs Candles
	n, err := parseCandles([]byte(jsonData), &cs)
	if err != nil {
		t.Errorf("%s: %s", prefix, err.Error())
	}
	if n != 3 {
		t.Errorf("%s: want len parsed 3, got: %d", prefix, n)
	}

	// float32 has low precision, test may cause the error
	var wantClosePrice float32 = 2507.23
	if cs[0].CPrice != wantClosePrice {
		t.Errorf("%s: want close price %f, got: %f", prefix, wantClosePrice, cs[0].CPrice)
	}

	var wantVolume float32 = 5.1033
	if cs[1].Volume != wantVolume {
		t.Errorf("%s: want volume %f, got: %f", prefix, wantVolume, cs[1].Volume)
	}

	var wantCloseTime uint32 = 1707696003
	if cs[2].CTime != wantCloseTime {
		t.Errorf("%s: want close time %d, got: %d", prefix, wantCloseTime, cs[2].CTime)
	}
}
