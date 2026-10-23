package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
	"github.com/Cypher-CP0/exchange-logger/internal/sources"
)

type memSink[T any] struct {
	mu   sync.Mutex
	rows []T
}

func (m *memSink[T]) Add(rows ...T) {
	m.mu.Lock()
	m.rows = append(m.rows, rows...)
	m.mu.Unlock()
}

type fixedPrices struct{ err error }

func (f fixedPrices) Snapshot(ctx context.Context, symbols []string) (prices.Snapshot, error) {
	if f.err != nil {
		return prices.Snapshot{}, f.err
	}
	return prices.Snapshot{
		TakenAt: time.Now(),
		USD:     map[string]float64{"SOL": 200, "ETH": 2500, "USDC": 1},
		Venues:  map[string]map[string]float64{"binance": {"SOL": 200, "ETH": 2500, "USDC": 1}},
	}, nil
}

// fakeSource returns 0.2% less than the input value, or a scripted error.
type fakeSource struct {
	name    string
	support func(model.Corridor) bool
	err     func(call int64) error
	calls   atomic.Int64
}

func (f *fakeSource) Name() string                   { return f.name }
func (f *fakeSource) Supports(c model.Corridor) bool { return f.support == nil || f.support(c) }
func (f *fakeSource) Quote(ctx context.Context, req sources.Request) (sources.Raw, error) {
	n := f.calls.Add(1)
	if f.err != nil {
		if err := f.err(n); err != nil {
			return nil, err
		}
	}
	return sources.Raw("ok"), nil
}
func (f *fakeSource) Normalize(req sources.Request, raw sources.Raw, snap prices.Snapshot) (sources.Result, error) {
	outPrice, _ := snap.Price(req.Corridor.To.PriceSymbol)
	outHuman := req.InputUSD * 0.998 / outPrice
	out := model.ToUnits(outHuman, req.Corridor.To.Decimals)
	gas := 0.5
	return sources.Result{RouteLabel: "fake", ExpectedOutput: out, MinOutput: new(big.Int).Div(out, big.NewInt(2)), GasUSDSrc: &gas}, nil
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testConfig(corridors []model.Corridor) Config {
	return Config{
		Interval:       2 * time.Second,
		RequestTimeout: 300 * time.Millisecond,
		PriceTimeout:   300 * time.Millisecond,
		SizeBuckets:    []int32{100, 1000},
		Corridors:      corridors,
		PauseAfter:     2,
		PauseFor:       time.Minute,
	}
}

func TestRoundWritesComparableRows(t *testing.T) {
	corridors := []model.Corridor{model.NewCorridor(model.BaseUSDC, model.SolUSDC), model.NewCorridor(model.SolSOL, model.SolUSDC)}
	a := &fakeSource{name: "a"}
	b := &fakeSource{name: "b", support: func(c model.Corridor) bool { return c.SameChain() }}
	quotes, priceRows := &memSink[model.Quote]{}, &memSink[model.PriceRow]{}
	c := New(testConfig(corridors), []SourceConfig{
		{Source: a, RPS: 100, Burst: 10, Concurrency: 2},
		{Source: b, RPS: 100, Burst: 10, Concurrency: 2},
	}, fixedPrices{}, quotes, priceRows, quiet())

	start := time.Now()
	stats := c.RunRound(context.Background(), start)
	// a: 2 corridors x 2 sizes, b: 1 corridor x 2 sizes
	if stats.Rows != 6 || stats.OK != 6 || len(quotes.rows) != 6 {
		t.Fatalf("stats %+v rows %d", stats, len(quotes.rows))
	}
	for _, q := range quotes.rows {
		if q.PollRound != start.UnixMilli() {
			t.Fatalf("poll_round %d != %d", q.PollRound, start.UnixMilli())
		}
		// 0.2% shortfall + $0.50 gas
		want := 20 + 0.5/float64(q.SizeBucket)*10000
		if q.CostBps == nil || abs(*q.CostBps-want) > 0.05 {
			t.Fatalf("%s %d: cost %v want %v", q.Corridor, q.SizeBucket, q.CostBps, want)
		}
		if q.QuoteID == "" || q.InputToken == "" || q.OutputUSD == nil || q.ExtraFeeUSD == nil {
			t.Fatalf("incomplete row %+v", q)
		}
	}
	// SOL input: $100 at $200 = 0.5 SOL
	for _, q := range quotes.rows {
		if q.Corridor == "sol-sol__sol-usdc" && q.SizeBucket == 100 && q.InputAmount.Big().String() != "500000000" {
			t.Fatalf("sol input amount %s", q.InputAmount.Big())
		}
	}
	if len(priceRows.rows) != 6 { // 3 binance + 3 ref
		t.Fatalf("price rows %d", len(priceRows.rows))
	}
}

func TestRoundSkippedWithoutPrices(t *testing.T) {
	quotes := &memSink[model.Quote]{}
	src := &fakeSource{name: "a"}
	c := New(testConfig(model.DefaultCorridors()), []SourceConfig{{Source: src, RPS: 100, Burst: 10}},
		fixedPrices{err: context.DeadlineExceeded}, quotes, &memSink[model.PriceRow]{}, quiet())
	stats := c.RunRound(context.Background(), time.Now())
	if !stats.Skipped || len(quotes.rows) != 0 || src.calls.Load() != 0 {
		t.Fatalf("round should be skipped without touching sources: %+v", stats)
	}
}

func TestFailuresAreRowsAndCircuitOpens(t *testing.T) {
	corridors := []model.Corridor{model.NewCorridor(model.BaseUSDC, model.SolUSDC)}
	src := &fakeSource{name: "flaky", err: func(int64) error { return &sources.Error{Code: "http_502", Detail: "bad gateway"} }}
	quotes := &memSink[model.Quote]{}
	c := New(testConfig(corridors), []SourceConfig{{Source: src, RPS: 100, Burst: 10}}, fixedPrices{}, quotes, &memSink[model.PriceRow]{}, quiet())

	for i := 0; i < 2; i++ { // PauseAfter = 2
		st := c.RunRound(context.Background(), time.Now())
		if st.Sources["flaky"].Errors["http_502"] != 2 {
			t.Fatalf("round %d: %+v", i, st.Sources["flaky"])
		}
	}
	if len(quotes.rows) != 4 || quotes.rows[0].QuoteOK || quotes.rows[0].ErrorCode != "http_502" || quotes.rows[0].ErrorDetail != "bad gateway" {
		t.Fatalf("failed rows %+v", quotes.rows[0])
	}
	calls := src.calls.Load()
	st := c.RunRound(context.Background(), time.Now())
	if !st.Sources["flaky"].Paused || src.calls.Load() != calls {
		t.Fatalf("source should be paused without being called: %+v", st.Sources["flaky"])
	}
	last := quotes.rows[len(quotes.rows)-1]
	if last.ErrorCode != sources.CodeSourcePaused {
		t.Fatalf("paused rounds must still be logged as unavailable, got %q", last.ErrorCode)
	}
}

func TestRateLimitStartsBackoff(t *testing.T) {
	corridors := []model.Corridor{model.NewCorridor(model.BaseUSDC, model.SolUSDC), model.NewCorridor(model.ArbUSDC, model.SolUSDC)}
	src := &fakeSource{name: "limited", err: func(n int64) error {
		if n == 1 {
			return &sources.Error{Code: sources.CodeRateLimited}
		}
		return nil
	}}
	quotes := &memSink[model.Quote]{}
	c := New(testConfig(corridors), []SourceConfig{{Source: src, RPS: 100, Burst: 10, Concurrency: 1}}, fixedPrices{}, quotes, &memSink[model.PriceRow]{}, quiet())
	st := c.RunRound(context.Background(), time.Now()).Sources["limited"]
	if st.Attempted != 1 || st.Errors[sources.CodeRateLimited] != 1 || st.SkippedBackoff != 3 {
		t.Fatalf("after a 429 the rest of the round should back off: %+v", st)
	}
	if len(quotes.rows) != 1 {
		t.Fatalf("skipped requests must not produce rows: %d", len(quotes.rows))
	}
}

func TestBudgetSkipsWhatDoesNotFit(t *testing.T) {
	corridors := []model.Corridor{model.NewCorridor(model.BaseUSDC, model.SolUSDC)}
	src := &fakeSource{name: "slow"}
	quotes := &memSink[model.Quote]{}
	// One token, refilling at 75 per 2 hours (LI.FI without a key).
	c := New(testConfig(corridors), []SourceConfig{{Source: src, RPS: 75.0 / 7200, Burst: 1}}, fixedPrices{}, quotes, &memSink[model.PriceRow]{}, quiet())
	st := c.RunRound(context.Background(), time.Now()).Sources["slow"]
	if st.Attempted != 1 || st.SkippedBudget != 1 || len(quotes.rows) != 1 {
		t.Fatalf("%+v rows=%d", st, len(quotes.rows))
	}
}

func TestBudgetLimitedSourceRotatesThroughJobs(t *testing.T) {
	// One token per round (e.g. LI.FI without a key). Each round must quote
	// the next corridor/size, not the first one again.
	corridors := []model.Corridor{model.NewCorridor(model.SolSOL, model.SolUSDC), model.NewCorridor(model.BaseUSDC, model.SolUSDC)}
	src := &fakeSource{name: "slow"}
	quotes := &memSink[model.Quote]{}
	c := New(testConfig(corridors), []SourceConfig{{Source: src, RPS: 1e-6, Burst: 1, Concurrency: 1}}, fixedPrices{}, quotes, &memSink[model.PriceRow]{}, quiet())

	seen := map[string]int{}
	for i := 0; i < 4; i++ { // 2 corridors x 2 sizes = 4 jobs
		c.sources[0].bucket.mu.Lock()
		c.sources[0].bucket.tokens = 1
		c.sources[0].bucket.mu.Unlock()
		before := len(quotes.rows)
		c.RunRound(context.Background(), time.Now())
		if len(quotes.rows) != before+1 {
			t.Fatalf("round %d: want exactly one quote, got %d", i, len(quotes.rows)-before)
		}
		q := quotes.rows[len(quotes.rows)-1]
		seen[fmt.Sprintf("%s/%d", q.Corridor, q.SizeBucket)]++
	}
	if len(seen) != 4 {
		t.Fatalf("4 rounds should cover all 4 jobs once, got %v", seen)
	}
}

func TestShutdownStopsNewRequests(t *testing.T) {
	corridors := []model.Corridor{model.NewCorridor(model.BaseUSDC, model.SolUSDC)}
	src := &fakeSource{name: "a"}
	c := New(testConfig(corridors), []SourceConfig{{Source: src, RPS: 100, Burst: 10}}, fixedPrices{}, &memSink[model.Quote]{}, &memSink[model.PriceRow]{}, quiet())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := c.RunRound(ctx, time.Now()).Sources["a"]
	if st.Attempted != 0 || st.SkippedShutdown != 2 {
		t.Fatalf("%+v", st)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
