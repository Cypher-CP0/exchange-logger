package prices

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func binanceServer(t *testing.T, body string, status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/ticker/price" {
			t.Errorf("binance path %s", r.URL.Path)
		}
		if !strings.Contains(r.URL.Query().Get("symbols"), `"SOLUSDT"`) {
			t.Errorf("binance symbols %q", r.URL.Query().Get("symbols"))
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
}

func okxServer(prices map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inst := r.URL.Query().Get("instId")
		p, ok := prices[inst]
		if !ok {
			w.Write([]byte(`{"code":"51001","msg":"Instrument ID does not exist","data":[]}`))
			return
		}
		w.Write([]byte(`{"code":"0","msg":"","data":[{"instId":"` + inst + `","last":"` + p + `"}]}`))
	}))
}

func TestMultiAveragesVenues(t *testing.T) {
	b := binanceServer(t, `[{"symbol":"SOLUSDT","price":"200.00"},{"symbol":"ETHUSDT","price":"2500.00"},{"symbol":"USDCUSDT","price":"1.0001"}]`, 200)
	defer b.Close()
	o := okxServer(map[string]string{"SOL-USDT": "202", "ETH-USDT": "2500", "USDC-USDT": "0.9999"})
	defer o.Close()

	var diverged []string
	m := &Multi{
		Venues:        []Venue{&Binance{BaseURL: b.URL, Client: b.Client()}, &OKX{BaseURL: o.URL, Client: o.Client()}},
		MaxDivergence: 0.005,
		OnDivergence:  func(sym string, _ map[string]float64) { diverged = append(diverged, sym) },
	}
	snap, err := m.Snapshot(context.Background(), []string{"ETH", "SOL", "USDC"})
	if err != nil {
		t.Fatal(err)
	}
	if snap.USD["SOL"] != 201 || snap.USD["ETH"] != 2500 || snap.USD["USDC"] != 1.0 {
		t.Fatalf("prices %v", snap.USD)
	}
	if len(diverged) != 1 || diverged[0] != "SOL" { // 1% apart > 0.5% threshold
		t.Fatalf("diverged %v", diverged)
	}
	if snap.Venues["binance"]["SOL"] != 200 || snap.Venues["okx"]["SOL"] != 202 {
		t.Fatalf("venues %v", snap.Venues)
	}
}

func TestMultiFallsBackWhenOneVenueIsDown(t *testing.T) {
	b := binanceServer(t, `{"code":0,"msg":"Service unavailable from a restricted location"}`, 451)
	defer b.Close()
	o := okxServer(map[string]string{"SOL-USDT": "202", "ETH-USDT": "2500", "USDC-USDT": "1"})
	defer o.Close()
	m := &Multi{Venues: []Venue{&Binance{BaseURL: b.URL, Client: b.Client()}, &OKX{BaseURL: o.URL, Client: o.Client()}}}
	snap, err := m.Snapshot(context.Background(), []string{"ETH", "SOL", "USDC"})
	if err != nil {
		t.Fatal(err)
	}
	if snap.USD["SOL"] != 202 {
		t.Fatalf("prices %v", snap.USD)
	}
}

func TestMultiFailsWhenASymbolIsMissing(t *testing.T) {
	o := okxServer(map[string]string{"SOL-USDT": "202", "USDC-USDT": "1"})
	defer o.Close()
	m := &Multi{Venues: []Venue{&OKX{BaseURL: o.URL, Client: o.Client()}}}
	_, err := m.Snapshot(context.Background(), []string{"ETH", "SOL", "USDC"})
	if err == nil || !strings.Contains(err.Error(), "ETH") {
		t.Fatalf("want missing-ETH error, got %v", err)
	}
}
