package sources

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
)

var snap = prices.Snapshot{USD: map[string]float64{"SOL": 200, "ETH": 2500, "USDC": 1}}

func approx(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: got nil, want %v", name, want)
	}
	if math.Abs(*got-want) > 1e-9 {
		t.Fatalf("%s: got %v, want %v", name, *got, want)
	}
}

const jupiterFixture = `{
  "mode": "ultra", "inputMint": "So11111111111111111111111111111111111111112",
  "outputMint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "inAmount": "5000000000", "outAmount": "998500000", "otherAmountThreshold": "993507500",
  "swapMode": "ExactIn", "slippageBps": 50, "priceImpact": -0.01,
  "routePlan": [
    {"swapInfo": {"ammKey": "a", "label": "Raydium CLMM", "inputMint": "x", "outputMint": "y", "inAmount": "1", "outAmount": "1"}, "percent": 60, "bps": 6000},
    {"swapInfo": {"ammKey": "b", "label": "Orca Whirlpool", "inputMint": "x", "outputMint": "y", "inAmount": "1", "outAmount": "1"}, "percent": 40, "bps": 4000},
    {"swapInfo": {"ammKey": "c", "label": "Raydium CLMM", "inputMint": "x", "outputMint": "y", "inAmount": "1", "outAmount": "1"}, "percent": 0, "bps": 0}
  ],
  "feeBps": 5, "signatureFeeLamports": 5000, "prioritizationFeeLamports": 95000, "rentFeeLamports": 2039280,
  "router": "metis", "transaction": null, "requestId": "req-1", "totalTime": 120
}`

func solReq() Request {
	c := model.NewCorridor(model.SolSOL, model.SolUSDC)
	return Request{PollRound: 1, Corridor: c, SizeBucket: 1000, InputAmount: model.ToUnits(5, 9), InputUSD: 1000}
}

func TestJupiterQuoteRequestAndNormalize(t *testing.T) {
	var gotQuery, gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery, gotKey = r.URL.RawQuery, r.Header.Get("x-api-key")
		w.Write([]byte(jupiterFixture))
	}))
	defer srv.Close()

	j := &Jupiter{OrderURL: srv.URL + "/swap/v2/order", APIKey: "k1", SlippageBps: 50, Client: srv.Client()}
	if !j.Supports(solReq().Corridor) || j.Supports(model.NewCorridor(model.BaseUSDC, model.SolUSDC)) {
		t.Fatal("Jupiter should support only same-chain Solana corridors")
	}
	raw, err := j.Quote(context.Background(), solReq())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"inputMint=So111", "outputMint=EPjF", "amount=5000000000", "slippageBps=50", "swapMode=ExactIn"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("query %q missing %q", gotQuery, want)
		}
	}
	if strings.Contains(gotQuery, "taker") {
		t.Error("must not send taker: we only want a quote, not a transaction")
	}
	if gotKey != "k1" {
		t.Errorf("x-api-key = %q", gotKey)
	}

	res, err := j.Normalize(solReq(), raw, snap)
	if err != nil {
		t.Fatal(err)
	}
	if res.ExpectedOutput.String() != "998500000" || res.MinOutput.String() != "993507500" {
		t.Fatalf("amounts: %v %v", res.ExpectedOutput, res.MinOutput)
	}
	if res.RouteLabel != "metis:Raydium CLMM+Orca Whirlpool" {
		t.Fatalf("route label %q", res.RouteLabel)
	}
	approx(t, "gas", res.GasUSDSrc, 100000.0/1e9*200) // signature + priority fee only, not rent
	approx(t, "fee", res.ProtocolFeeUSD, 0.5)         // 5 bps of $1000, informational
	if res.ExtraFeeUSD != 0 || res.GasUSDDst != nil || res.EstSeconds != nil {
		t.Fatalf("unexpected extra fields: %+v", res)
	}
}

func TestJupiterErrors(t *testing.T) {
	cases := []struct {
		status int
		body   string
		header map[string]string
		code   string
	}{
		{400, `{"error":"Could not find any route","requestId":"r"}`, nil, CodeNoRoute},
		{400, `{"error":"Invalid inputMint","requestId":"r"}`, nil, "http_400"},
		{401, `{"error":"unauthorized"}`, nil, CodeUnauthorized},
		{429, `{"error":"slow down"}`, map[string]string{"Retry-After": "7"}, CodeRateLimited},
		{503, `upstream down`, nil, "http_503"},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range tc.header {
				w.Header().Set(k, v)
			}
			w.WriteHeader(tc.status)
			w.Write([]byte(tc.body))
		}))
		j := &Jupiter{OrderURL: srv.URL, Client: srv.Client()}
		_, err := j.Quote(context.Background(), solReq())
		srv.Close()
		e := Classify(err)
		if e.Code != tc.code {
			t.Errorf("status %d %s: code %q, want %q", tc.status, tc.body, e.Code, tc.code)
		}
		if tc.status == 429 && e.RetryAfter != 7*time.Second {
			t.Errorf("Retry-After not parsed: %v", e.RetryAfter)
		}
	}

	// A 200 with outAmount 0 is not a usable quote.
	j := &Jupiter{}
	if _, err := j.Normalize(solReq(), Raw(`{"inAmount":"1","outAmount":"0"}`), snap); Classify(err).Code != CodeNoRoute {
		t.Errorf("zero output: %v", err)
	}
	if _, err := j.Normalize(solReq(), Raw(`not json`), snap); Classify(err).Code != CodeBadResponse {
		t.Errorf("bad json: %v", err)
	}
}

func TestJupiterTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	j := &Jupiter{OrderURL: srv.URL, Client: srv.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := j.Quote(ctx, solReq())
	if Classify(err).Code != CodeTimeout {
		t.Fatalf("got %v, want timeout", err)
	}
}

const lifiFixture = `{
  "type": "lifi", "id": "q-1", "tool": "mayan",
  "toolDetails": {"key": "mayan", "name": "Mayan"},
  "action": {"fromChainId": 8453, "toChainId": 1151111081099710},
  "estimate": {
    "tool": "mayan", "fromAmount": "1000000000", "toAmount": "998123456", "toAmountMin": "993132839",
    "approvalAddress": "0x0", "executionDuration": 42.6,
    "feeCosts": [
      {"name": "Relayer fee", "token": {"symbol": "USDC", "decimals": 6, "priceUSD": "0.9998"},
       "amount": "500000", "amountUSD": "0.50", "percentage": "0.0005", "included": true},
      {"name": "Bridge native fee", "token": {"symbol": "ETH", "decimals": 18, "priceUSD": "3000"},
       "amount": "100000000000000", "amountUSD": "0.30", "included": false},
      {"name": "Odd token fee", "token": {"symbol": "XYZ", "decimals": 18},
       "amount": "1", "amountUSD": "0.05", "included": true}
    ],
    "gasCosts": [
      {"type": "SEND", "price": "1", "estimate": "1", "limit": "1",
       "amount": "200000000000000", "amountUSD": "0.60", "token": {"symbol": "ETH", "decimals": 18, "priceUSD": "3000"}}
    ]
  },
  "includedSteps": [
    {"type": "protocol", "tool": "feeCollection"},
    {"type": "cross", "tool": "mayan"}
  ]
}`

func baseReq() Request {
	c := model.NewCorridor(model.BaseUSDC, model.SolUSDC)
	return Request{PollRound: 1, Corridor: c, SizeBucket: 1000, InputAmount: model.ToUnits(1000, 6), InputUSD: 1000}
}

func TestLiFiQuoteRequestAndNormalize(t *testing.T) {
	var q map[string]string
	var key string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/quote" {
			t.Errorf("path %s", r.URL.Path)
		}
		q = map[string]string{}
		for k, v := range r.URL.Query() {
			q[k] = v[0]
		}
		key = r.Header.Get("x-lifi-api-key")
		w.Write([]byte(lifiFixture))
	}))
	defer srv.Close()

	l := &LiFi{BaseURL: srv.URL, APIKey: "lk", SlippageBps: 50, Order: "CHEAPEST",
		EVMAddress: "0xEVM", SolanaAddress: "SolAddr", Client: srv.Client()}
	raw, err := l.Quote(context.Background(), baseReq())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"fromChain": "8453", "toChain": "1151111081099710",
		"fromToken": model.BaseUSDC.Address, "toToken": model.SolUSDC.Address,
		"fromAmount": "1000000000", "fromAddress": "0xEVM", "toAddress": "SolAddr",
		"slippage": "0.005", "order": "CHEAPEST",
	}
	for k, v := range want {
		if q[k] != v {
			t.Errorf("%s = %q, want %q", k, q[k], v)
		}
	}
	if _, ok := q["integrator"]; ok {
		t.Error("integrator should be omitted when unset")
	}
	if key != "lk" {
		t.Errorf("x-lifi-api-key = %q", key)
	}

	res, err := l.Normalize(baseReq(), raw, snap)
	if err != nil {
		t.Fatal(err)
	}
	if res.ExpectedOutput.String() != "998123456" || res.MinOutput.String() != "993132839" {
		t.Fatalf("amounts %v %v", res.ExpectedOutput, res.MinOutput)
	}
	if res.RouteLabel != "mayan" {
		t.Fatalf("route label %q", res.RouteLabel)
	}
	approx(t, "gas (re-valued at snapshot ETH=2500)", res.GasUSDSrc, 0.0002*2500)
	// USDC 0.5 + ETH 0.0001*2500 + unknown token at LI.FI's amountUSD 0.05
	approx(t, "protocol fee", res.ProtocolFeeUSD, 0.5+0.25+0.05)
	if math.Abs(res.ExtraFeeUSD-0.25) > 1e-9 {
		t.Fatalf("extra fee (included=false only) = %v", res.ExtraFeeUSD)
	}
	if res.EstSeconds == nil || *res.EstSeconds != 43 {
		t.Fatalf("est seconds %v", res.EstSeconds)
	}
	if res.GasUSDDst != nil {
		t.Fatal("LI.FI does not report destination gas")
	}
}

func TestLiFiNativeTokensAndSolanaOrigin(t *testing.T) {
	var q map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q = map[string]string{}
		for k, v := range r.URL.Query() {
			q[k] = v[0]
		}
		w.Write([]byte(lifiFixture))
	}))
	defer srv.Close()
	l := &LiFi{BaseURL: srv.URL, EVMAddress: "0xEVM", SolanaAddress: "SolAddr", Client: srv.Client()}

	req := solReq()
	if _, err := l.Quote(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if q["fromToken"] != "11111111111111111111111111111111" || q["fromAddress"] != "SolAddr" || q["toAddress"] != "SolAddr" {
		t.Errorf("solana native: %v", q)
	}

	req.Corridor = model.NewCorridor(model.BaseETH, model.SolUSDC)
	if _, err := l.Quote(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if q["fromToken"] != "0x0000000000000000000000000000000000000000" || q["fromAddress"] != "0xEVM" || q["toAddress"] != "SolAddr" {
		t.Errorf("base native eth: %v", q)
	}
}

func TestLiFiErrors(t *testing.T) {
	cases := []struct {
		status int
		body   string
		code   string
	}{
		{404, `{"message":"No available quotes for the requested transfer","code":1002}`, CodeNoRoute},
		{400, `{"message":"Invalid fromToken","code":1011}`, "http_400"},
		{429, `{"message":"Too many requests","code":1005}`, CodeRateLimited},
		{500, `{"message":"boom","code":1006}`, "http_500"},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			w.Write([]byte(tc.body))
		}))
		l := &LiFi{BaseURL: srv.URL, Client: srv.Client()}
		_, err := l.Quote(context.Background(), baseReq())
		srv.Close()
		if got := Classify(err).Code; got != tc.code {
			t.Errorf("%d %s: got %q want %q", tc.status, tc.body, got, tc.code)
		}
	}
}
