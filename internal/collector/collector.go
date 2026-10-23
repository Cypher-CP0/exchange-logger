// Package collector runs the polling rounds: take one price snapshot, ask
// every source for every corridor and size it supports, normalize the
// answers into quote rows, and hand them to the sink.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
	"github.com/Cypher-CP0/exchange-logger/internal/sources"
)

// Sink receives rows; the Parquet writer implements it.
type Sink[T any] interface {
	Add(rows ...T)
}

// SourceConfig wires a source to its rate budget.
type SourceConfig struct {
	Source      sources.Source
	RPS         float64 // sustained requests per second
	Burst       int
	Concurrency int // max in-flight requests
}

// Config controls round timing and the circuit breaker.
type Config struct {
	Interval       time.Duration // time between round starts (30s)
	RequestTimeout time.Duration // per quote request (5s)
	PriceTimeout   time.Duration // for the price snapshot (5s)
	SizeBuckets    []int32       // USD sizes
	Corridors      []model.Corridor
	PauseAfter     int           // failed rounds in a row before a source is paused (10)
	PauseFor       time.Duration // how long it stays paused (5m)
	StatusPath     string        // collector status JSON; empty disables it
}

type sourceState struct {
	cfg    SourceConfig
	bucket *tokenBucket
	health sourceHealth
	// cursor rotates where each round starts in this source's job list.
	// When the rate budget only covers part of a round, the next round
	// picks up where this one stopped instead of re-quoting the first
	// corridor and size forever. Only RunRound touches it.
	cursor int
}

// rotate returns jobs starting at offset, wrapping around.
func rotate(jobs []job, offset int) []job {
	n := len(jobs)
	if n == 0 {
		return jobs
	}
	off := offset % n
	out := make([]job, 0, n)
	out = append(out, jobs[off:]...)
	return append(out, jobs[:off]...)
}

// Collector owns the round loop.
type Collector struct {
	cfg     Config
	sources []*sourceState
	prices  prices.Provider
	quotes  Sink[model.Quote]
	priceDB Sink[model.PriceRow]
	log     *slog.Logger
	now     func() time.Time

	startedAt time.Time
	statusMu  sync.Mutex
}

// New builds a collector.
func New(cfg Config, srcs []SourceConfig, p prices.Provider, quotes Sink[model.Quote], priceRows Sink[model.PriceRow], log *slog.Logger) *Collector {
	c := &Collector{cfg: cfg, prices: p, quotes: quotes, priceDB: priceRows, log: log, now: time.Now}
	start := c.now()
	for _, s := range srcs {
		if s.Concurrency < 1 {
			s.Concurrency = 1
		}
		c.sources = append(c.sources, &sourceState{cfg: s, bucket: newTokenBucket(s.RPS, s.Burst, start)})
	}
	return c
}

// Run starts a round immediately and then every Interval until ctx is
// cancelled. On cancellation the current round stops starting new requests,
// lets in-flight ones finish, and still hands its rows to the sink.
func (c *Collector) Run(ctx context.Context) {
	c.startedAt = c.now()
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		c.RunRound(ctx, c.now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// The ticker drops ticks if a round overruns, so rounds never overlap.
		if ctx.Err() != nil {
			return
		}
	}
}

// RoundStats summarizes one round for logs and the status file.
type RoundStats struct {
	PollRound  int64                   `json:"poll_round"`
	StartedAt  time.Time               `json:"started_at"`
	DurationMs int64                   `json:"duration_ms"`
	Skipped    bool                    `json:"skipped"`
	SkipReason string                  `json:"skip_reason,omitempty"`
	Rows       int                     `json:"rows"`
	OK         int                     `json:"ok"`
	Sources    map[string]*SourceStats `json:"sources"`
}

// SourceStats are per-source counts for one round.
type SourceStats struct {
	Attempted       int            `json:"attempted"` // requests actually sent
	OK              int            `json:"ok"`
	Errors          map[string]int `json:"errors,omitempty"`
	SkippedBudget   int            `json:"skipped_budget,omitempty"`  // rate budget couldn't fit the request in this round
	SkippedBackoff  int            `json:"skipped_backoff,omitempty"` // backing off after a 429
	SkippedShutdown int            `json:"skipped_shutdown,omitempty"`
	Paused          bool           `json:"paused,omitempty"`
	MedianLatency   int32          `json:"median_latency_ms,omitempty"`
}

type job struct {
	src  *sourceState
	req  sources.Request
	snap prices.Snapshot
}

// RunRound runs one polling round and returns its stats. Cancelling ctx
// stops new requests; requests already sent run to their own timeout.
func (c *Collector) RunRound(ctx context.Context, start time.Time) RoundStats {
	pollRound := start.UnixMilli()
	stats := RoundStats{PollRound: pollRound, StartedAt: start.UTC(), Sources: map[string]*SourceStats{}}
	deadline := start.Add(c.cfg.Interval - c.cfg.Interval/20)
	roundCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()

	// 1. One shared price snapshot. Without it the round is skipped:
	//    costing quotes against stale or guessed prices would corrupt rankings.
	pctx, pcancel := context.WithTimeout(roundCtx, c.cfg.PriceTimeout)
	snap, err := c.prices.Snapshot(pctx, model.PriceSymbols(c.cfg.Corridors))
	pcancel()
	if err != nil {
		stats.Skipped, stats.SkipReason = true, "price_snapshot_failed"
		stats.DurationMs = c.now().Sub(start).Milliseconds()
		c.log.Warn("round skipped", "event", "round_skipped", "poll_round", pollRound, "reason", stats.SkipReason, "error", err.Error())
		c.writeStatus(stats)
		return stats
	}
	c.priceDB.Add(priceRows(pollRound, snap)...)

	// 2. Build the jobs: every source x supported corridor x size.
	perSource := map[*sourceState][]job{}
	for _, s := range c.sources {
		stats.Sources[s.cfg.Source.Name()] = &SourceStats{Errors: map[string]int{}}
		for _, cor := range c.cfg.Corridors {
			if !s.cfg.Source.Supports(cor) {
				continue
			}
			for _, size := range c.cfg.SizeBuckets {
				req, err := buildRequest(pollRound, cor, size, snap)
				if err != nil {
					c.log.Error("cannot build request", "event", "request_build_failed", "corridor", cor.ID, "size_bucket", size, "error", err.Error())
					continue
				}
				perSource[s] = append(perSource[s], job{src: s, req: req, snap: snap})
			}
		}
	}

	// 3. Run each source's jobs through its own worker pool and budget.
	var mu sync.Mutex
	var rows []model.Quote
	latencies := map[string][]int32{}
	var wg sync.WaitGroup
	for s, jobs := range perSource {
		st := stats.Sources[s.cfg.Source.Name()]
		if s.health.paused(start) {
			// Paused sources still get rows, so the pause shows up as
			// unavailability in the rankings.
			mu.Lock()
			st.Paused = true
			for _, j := range jobs {
				rows = append(rows, c.failedRow(j.req, s.cfg.Source.Name(), c.now(), 0, &sources.Error{Code: sources.CodeSourcePaused}))
				st.Errors[sources.CodeSourcePaused]++
			}
			mu.Unlock()
			continue
		}
		jobs = rotate(jobs, s.cursor)
		ch := make(chan job)
		for i := 0; i < s.cfg.Concurrency; i++ {
			wg.Add(1)
			go func(s *sourceState, st *SourceStats) {
				defer wg.Done()
				for j := range ch {
					row, outcome := c.runJob(ctx, roundCtx, deadline, j)
					mu.Lock()
					switch outcome {
					case outcomeSkippedBudget:
						st.SkippedBudget++
					case outcomeSkippedBackoff:
						st.SkippedBackoff++
					case outcomeSkippedShutdown:
						st.SkippedShutdown++
					default:
						st.Attempted++
						rows = append(rows, row)
						latencies[s.cfg.Source.Name()] = append(latencies[s.cfg.Source.Name()], row.LatencyMs)
						if row.QuoteOK {
							st.OK++
						} else {
							st.Errors[row.ErrorCode]++
						}
					}
					mu.Unlock()
				}
			}(s, st)
		}
		wg.Add(1)
		go func(jobs []job, ch chan job) {
			defer wg.Done()
			defer close(ch)
			for _, j := range jobs {
				ch <- j
			}
		}(jobs, ch)
	}
	wg.Wait()

	// 4. Circuit breaker bookkeeping, then hand rows to the sink.
	for s, jobs := range perSource {
		st := stats.Sources[s.cfg.Source.Name()]
		st.MedianLatency = median(latencies[s.cfg.Source.Name()])
		if len(jobs) > 0 {
			s.cursor = (s.cursor + st.Attempted) % len(jobs)
		}
		if opened, until := s.health.endRound(c.now(), st.Attempted, st.OK, c.cfg.PauseAfter, c.cfg.PauseFor); opened {
			c.log.Warn("source paused after repeated failed rounds", "event", "source_paused",
				"source", s.cfg.Source.Name(), "failed_rounds", c.cfg.PauseAfter, "paused_until", until.UTC().Format(time.RFC3339))
		}
	}
	sort.Slice(rows, func(i, k int) bool { return rows[i].TS.Before(rows[k].TS) })
	c.quotes.Add(rows...)

	stats.Rows = len(rows)
	for _, r := range rows {
		if r.QuoteOK {
			stats.OK++
		}
	}
	stats.DurationMs = c.now().Sub(start).Milliseconds()
	attrs := []any{"event", "round_complete", "poll_round", pollRound, "rows", stats.Rows, "ok", stats.OK, "duration_ms", stats.DurationMs}
	for name, st := range stats.Sources {
		attrs = append(attrs, slog.Group(name, "attempted", st.Attempted, "ok", st.OK, "errors", st.Errors,
			"skipped_budget", st.SkippedBudget, "skipped_backoff", st.SkippedBackoff, "skipped_shutdown", st.SkippedShutdown,
			"paused", st.Paused, "median_latency_ms", st.MedianLatency))
	}
	c.log.Info("round complete", attrs...)
	c.writeStatus(stats)
	return stats
}

type outcome int

const (
	outcomeAttempted outcome = iota
	outcomeSkippedBudget
	outcomeSkippedBackoff
	outcomeSkippedShutdown
)

func (c *Collector) runJob(stop, roundCtx context.Context, deadline time.Time, j job) (model.Quote, outcome) {
	s := j.src
	name := s.cfg.Source.Name()
	if stop.Err() != nil {
		return model.Quote{}, outcomeSkippedShutdown
	}
	if s.health.inBackoff(c.now()) {
		return model.Quote{}, outcomeSkippedBackoff
	}
	// Wait for the rate budget, but only if the request still fits in the round.
	wait := s.bucket.Reserve(c.now())
	if c.now().Add(wait).Add(c.cfg.RequestTimeout).After(deadline) {
		s.bucket.Cancel()
		return model.Quote{}, outcomeSkippedBudget
	}
	if wait > 0 {
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-roundCtx.Done():
			t.Stop()
			s.bucket.Cancel()
			return model.Quote{}, outcomeSkippedBudget
		case <-stop.Done():
			t.Stop()
			s.bucket.Cancel()
			return model.Quote{}, outcomeSkippedShutdown
		}
	}
	if s.health.inBackoff(c.now()) { // a 429 may have arrived while we waited
		return model.Quote{}, outcomeSkippedBackoff
	}

	reqCtx, cancel := context.WithTimeout(roundCtx, c.cfg.RequestTimeout)
	defer cancel()
	t0 := c.now()
	raw, err := s.cfg.Source.Quote(reqCtx, j.req)
	received := c.now()
	latency := int32(received.Sub(t0).Milliseconds())
	if err != nil {
		se := sources.Classify(err)
		if se.Code == sources.CodeTimeout && se.Detail == "" {
			se = &sources.Error{Code: se.Code, Detail: "no response within " + c.cfg.RequestTimeout.String()}
		}
		if se.Code == sources.CodeRateLimited {
			until := s.health.onRateLimited(c.now(), se.RetryAfter)
			c.log.Warn("source rate limited", "event", "source_rate_limited", "source", name, "backoff_until", until.UTC().Format(time.RFC3339))
		}
		return c.failedRow(j.req, name, received, latency, se), outcomeAttempted
	}
	s.health.onSuccess()
	res, err := s.cfg.Source.Normalize(j.req, raw, j.snap)
	if err != nil {
		return c.failedRow(j.req, name, received, latency, sources.Classify(err)), outcomeAttempted
	}
	row, err := c.okRow(j.req, name, received, latency, res, j.snap)
	if err != nil {
		return c.failedRow(j.req, name, received, latency, &sources.Error{Code: sources.CodeBadResponse, Detail: err.Error()}), outcomeAttempted
	}
	return row, outcomeAttempted
}

func buildRequest(pollRound int64, cor model.Corridor, size int32, snap prices.Snapshot) (sources.Request, error) {
	p, ok := snap.Price(cor.From.PriceSymbol)
	if !ok {
		return sources.Request{}, fmt.Errorf("no price for %s", cor.From.PriceSymbol)
	}
	amount := model.ToUnits(float64(size)/p, cor.From.Decimals)
	if amount.Sign() <= 0 {
		return sources.Request{}, fmt.Errorf("input amount rounds to zero")
	}
	return sources.Request{
		PollRound:   pollRound,
		Corridor:    cor,
		SizeBucket:  size,
		InputAmount: amount,
		InputUSD:    model.FromUnits(amount, cor.From.Decimals) * p,
	}, nil
}

func (c *Collector) baseRow(req sources.Request, source string, ts time.Time, latency int32) model.Quote {
	in, _ := model.DecimalFromBig(req.InputAmount)
	return model.Quote{
		QuoteID:     uuid.Must(uuid.NewV7()).String(),
		PollRound:   req.PollRound,
		TS:          ts.UTC(),
		Source:      source,
		Corridor:    req.Corridor.ID,
		SizeBucket:  req.SizeBucket,
		InputToken:  req.Corridor.From.Address,
		OutputToken: req.Corridor.To.Address,
		InputAmount: in,
		InputUSD:    req.InputUSD,
		LatencyMs:   latency,
	}
}

func (c *Collector) failedRow(req sources.Request, source string, ts time.Time, latency int32, e *sources.Error) model.Quote {
	q := c.baseRow(req, source, ts, latency)
	q.QuoteOK = false
	q.ErrorCode = e.Code
	q.ErrorDetail = truncate(e.Detail, 300)
	return q
}

func (c *Collector) okRow(req sources.Request, source string, ts time.Time, latency int32, res sources.Result, snap prices.Snapshot) (model.Quote, error) {
	q := c.baseRow(req, source, ts, latency)
	outPrice, ok := snap.Price(req.Corridor.To.PriceSymbol)
	if !ok {
		return q, fmt.Errorf("no price for %s", req.Corridor.To.PriceSymbol)
	}
	exp, err := model.DecimalFromBig(res.ExpectedOutput)
	if err != nil {
		return q, fmt.Errorf("expected_output: %w", err)
	}
	min, err := model.DecimalFromBig(res.MinOutput)
	if err != nil {
		return q, fmt.Errorf("min_output: %w", err)
	}
	outUSD := model.FromUnits(res.ExpectedOutput, req.Corridor.To.Decimals) * outPrice
	cost := model.CostBps(req.InputUSD, outUSD, res.GasUSDSrc, res.GasUSDDst, res.ExtraFeeUSD)
	if math.IsNaN(cost) || math.IsInf(cost, 0) {
		return q, fmt.Errorf("cost_bps is not finite")
	}
	q.RouteLabel = res.RouteLabel
	q.ExpectedOutput = exp
	q.MinOutput = min
	q.OutputUSD = model.Ptr(outUSD)
	q.GasUSDSrc = res.GasUSDSrc
	q.GasUSDDst = res.GasUSDDst
	q.ProtocolFeeUSD = res.ProtocolFeeUSD
	q.ExtraFeeUSD = model.Ptr(res.ExtraFeeUSD)
	q.EstSeconds = res.EstSeconds
	q.CostBps = model.Ptr(model.Round2(cost))
	q.QuoteOK = true
	return q, nil
}

func priceRows(pollRound int64, snap prices.Snapshot) []model.PriceRow {
	var out []model.PriceRow
	for venue, ps := range snap.Venues {
		for sym, p := range ps {
			out = append(out, model.PriceRow{PollRound: pollRound, TS: snap.TakenAt, Symbol: sym, Venue: venue, PriceUSD: p})
		}
	}
	for sym, p := range snap.USD {
		out = append(out, model.PriceRow{PollRound: pollRound, TS: snap.TakenAt, Symbol: sym, Venue: "ref", PriceUSD: p})
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].Symbol != out[k].Symbol {
			return out[i].Symbol < out[k].Symbol
		}
		return out[i].Venue < out[k].Venue
	})
	return out
}

func median(v []int32) int32 {
	if len(v) == 0 {
		return 0
	}
	s := append([]int32(nil), v...)
	sort.Slice(s, func(i, k int) bool { return s[i] < s[k] })
	return s[len(s)/2]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// writeStatus atomically writes the last round's stats so the rankings API
// can report collector freshness without talking to the collector.
func (c *Collector) writeStatus(stats RoundStats) {
	if c.cfg.StatusPath == "" {
		return
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	doc := struct {
		StartedAt time.Time  `json:"collector_started_at"`
		UpdatedAt time.Time  `json:"updated_at"`
		LastRound RoundStats `json:"last_round"`
	}{c.startedAt.UTC(), c.now().UTC(), stats}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(c.cfg.StatusPath), 0o755); err != nil {
		c.log.Warn("cannot write status", "event", "status_write_failed", "error", err.Error())
		return
	}
	tmp := c.cfg.StatusPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		c.log.Warn("cannot write status", "event", "status_write_failed", "error", err.Error())
		return
	}
	if err := os.Rename(tmp, c.cfg.StatusPath); err != nil {
		c.log.Warn("cannot write status", "event", "status_write_failed", "error", err.Error())
	}
}
