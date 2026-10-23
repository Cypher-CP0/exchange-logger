// Package prices takes the reference price snapshot each polling round is
// costed against. Every quote in a round uses the same snapshot, which is
// what makes quotes in one round directly comparable.
//
// Prices come from CEX public tickers quoted in USDT, and USDT is treated as
// $1. That is close enough for ranking: an error in the reference price
// shifts every source in a round by the same amount.
package prices

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Snapshot is the set of USD prices for one round.
type Snapshot struct {
	TakenAt time.Time
	USD     map[string]float64            // symbol -> reference price used for costing
	Venues  map[string]map[string]float64 // venue -> symbol -> price, as reported
}

// Price returns the reference USD price for a symbol.
func (s Snapshot) Price(symbol string) (float64, bool) {
	p, ok := s.USD[symbol]
	return p, ok && p > 0
}

// Provider produces a snapshot covering every requested symbol, or fails.
type Provider interface {
	Snapshot(ctx context.Context, symbols []string) (Snapshot, error)
}

// Venue is one price feed.
type Venue interface {
	Name() string
	Fetch(ctx context.Context, symbols []string) (map[string]float64, error)
}

// Multi queries all venues in parallel and uses the mean of whichever
// returned each symbol. A round needs every symbol priced; if one is missing
// the snapshot fails and the collector skips the round rather than costing
// quotes against a guess.
type Multi struct {
	Venues []Venue
	// MaxDivergence is the relative spread between venues above which a
	// warning is reported (0.01 = 1%).
	MaxDivergence float64
	// OnDivergence is called when venues disagree by more than MaxDivergence.
	OnDivergence func(symbol string, prices map[string]float64)
	Now          func() time.Time
}

func (m *Multi) Snapshot(ctx context.Context, symbols []string) (Snapshot, error) {
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	snap := Snapshot{TakenAt: now().UTC(), USD: map[string]float64{}, Venues: map[string]map[string]float64{}}

	type result struct {
		venue  string
		prices map[string]float64
		err    error
	}
	results := make([]result, len(m.Venues))
	var wg sync.WaitGroup
	for i, v := range m.Venues {
		wg.Add(1)
		go func(i int, v Venue) {
			defer wg.Done()
			p, err := v.Fetch(ctx, symbols)
			results[i] = result{venue: v.Name(), prices: p, err: err}
		}(i, v)
	}
	wg.Wait()

	var errs []string
	for _, r := range results {
		if r.err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", r.venue, r.err))
			continue
		}
		snap.Venues[r.venue] = r.prices
	}

	var missing []string
	for _, sym := range symbols {
		per := map[string]float64{}
		for venue, ps := range snap.Venues {
			if p, ok := ps[sym]; ok && p > 0 && !math.IsInf(p, 0) && !math.IsNaN(p) {
				per[venue] = p
			}
		}
		if len(per) == 0 {
			missing = append(missing, sym)
			continue
		}
		var sum, lo, hi float64
		lo = math.MaxFloat64
		for _, p := range per {
			sum += p
			lo = math.Min(lo, p)
			hi = math.Max(hi, p)
		}
		mean := sum / float64(len(per))
		snap.USD[sym] = mean
		if m.MaxDivergence > 0 && len(per) > 1 && (hi-lo)/mean > m.MaxDivergence && m.OnDivergence != nil {
			m.OnDivergence(sym, per)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		msg := "no price for " + strings.Join(missing, ", ")
		if len(errs) > 0 {
			msg += " (" + strings.Join(errs, "; ") + ")"
		}
		return snap, fmt.Errorf("%s", msg)
	}
	return snap, nil
}

// tickerSymbol maps a symbol to the exchange base asset; every pair is quoted
// in USDT, which is treated as $1.
func tickerSymbol(sym string) string { return strings.ToUpper(sym) }

// Binance reads https://api.binance.com/api/v3/ticker/price for SYMBOLUSDT pairs.
type Binance struct {
	BaseURL string
	Client  *http.Client
}

func (b *Binance) Name() string { return "binance" }

func (b *Binance) Fetch(ctx context.Context, symbols []string) (map[string]float64, error) {
	pairs := make([]string, 0, len(symbols))
	for _, s := range symbols {
		pairs = append(pairs, `"`+tickerSymbol(s)+`USDT"`)
	}
	u := strings.TrimRight(b.BaseURL, "/") + "/api/v3/ticker/price?symbols=" + url.QueryEscape("["+strings.Join(pairs, ",")+"]")
	body, err := get(ctx, b.Client, u)
	if err != nil {
		return nil, err
	}
	var rows []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	out := map[string]float64{}
	for _, r := range rows {
		sym := strings.TrimSuffix(r.Symbol, "USDT")
		if p, err := strconv.ParseFloat(r.Price, 64); err == nil {
			out[sym] = p
		}
	}
	return out, nil
}

// OKX reads https://www.okx.com/api/v5/market/ticker, one SYMBOL-USDT pair per request.
type OKX struct {
	BaseURL string
	Client  *http.Client
}

func (o *OKX) Name() string { return "okx" }

func (o *OKX) Fetch(ctx context.Context, symbols []string) (map[string]float64, error) {
	out := map[string]float64{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for _, s := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			u := strings.TrimRight(o.BaseURL, "/") + "/api/v5/market/ticker?instId=" + url.QueryEscape(tickerSymbol(sym)+"-USDT")
			body, err := get(ctx, o.Client, u)
			var resp struct {
				Code string `json:"code"`
				Msg  string `json:"msg"`
				Data []struct {
					Last string `json:"last"`
				} `json:"data"`
			}
			if err == nil {
				err = json.Unmarshal(body, &resp)
			}
			if err == nil && (resp.Code != "0" || len(resp.Data) == 0) {
				err = fmt.Errorf("%s: code %s %s", sym, resp.Code, resp.Msg)
			}
			var p float64
			if err == nil {
				p, err = strconv.ParseFloat(resp.Data[0].Last, 64)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			out[sym] = p
		}(s)
	}
	wg.Wait()
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func get(ctx context.Context, client *http.Client, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
