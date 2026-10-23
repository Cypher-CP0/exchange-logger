// Command collector polls quote sources on a fixed cadence and writes every
// quote, successful or not, to Hive-partitioned Parquet files.
//
// It is a worker, not a server: nothing can trigger it over HTTP. The
// rankings API (a separate binary) reads what the ranker computes from the
// files this writes.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/collector"
	"github.com/Cypher-CP0/exchange-logger/internal/config"
	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
	"github.com/Cypher-CP0/exchange-logger/internal/sink"
	"github.com/Cypher-CP0/exchange-logger/internal/sources"
)

func main() {
	if err := config.LoadDotEnv(".env"); err != nil {
		fmt.Fprintln(os.Stderr, "reading .env:", err)
		os.Exit(1)
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:\n"+err.Error())
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "exchange-logger-collector")

	if err := run(cfg, log); err != nil {
		log.Error("collector stopped", "event", "collector_failed", "error", err.Error())
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	client := &http.Client{Transport: &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}}

	srcs := buildSources(cfg, client, log)
	if len(srcs) == 0 {
		return fmt.Errorf("no quote sources enabled")
	}
	warnBudgets(cfg, srcs, log)

	var venues []prices.Venue
	for _, v := range cfg.PriceVenues {
		switch v {
		case "binance":
			venues = append(venues, &prices.Binance{BaseURL: cfg.BinanceURL, Client: client})
		case "okx":
			venues = append(venues, &prices.OKX{BaseURL: cfg.OKXURL, Client: client})
		}
	}
	priceProvider := &prices.Multi{
		Venues:        venues,
		MaxDivergence: 0.01,
		OnDivergence: func(sym string, per map[string]float64) {
			log.Warn("price venues disagree by more than 1%", "event", "price_divergence", "symbol", sym, "prices", per)
		},
	}

	quotes := sink.NewPartitioned(filepath.Join(cfg.DataDir, "quotes"), func(q model.Quote) string {
		return sink.DatePartition(q.TS) + "/corridor=" + q.Corridor
	}, cfg.FlushMaxRows, log)
	priceRows := sink.NewPartitioned(filepath.Join(cfg.DataDir, "prices"), func(p model.PriceRow) string {
		return sink.DatePartition(p.TS)
	}, cfg.FlushMaxRows, log)

	col := collector.New(collector.Config{
		Interval:       cfg.PollInterval,
		RequestTimeout: cfg.RequestTimeout,
		PriceTimeout:   cfg.RequestTimeout,
		SizeBuckets:    cfg.SizeBuckets,
		Corridors:      cfg.Corridors,
		PauseAfter:     cfg.PauseAfter,
		PauseFor:       cfg.PauseFor,
		StatusPath:     filepath.Join(cfg.DataDir, "status", "collector.json"),
	}, srcs, priceProvider, quotes, priceRows, log)

	corridorIDs := make([]string, 0, len(cfg.Corridors))
	for _, c := range cfg.Corridors {
		corridorIDs = append(corridorIDs, c.ID)
	}
	names := make([]string, 0, len(srcs))
	for _, s := range srcs {
		names = append(names, s.Source.Name())
	}
	log.Info("collector starting", "event", "collector_started",
		"sources", names, "corridors", corridorIDs, "size_buckets", cfg.SizeBuckets,
		"poll_interval", cfg.PollInterval.String(), "flush_interval", cfg.FlushInterval.String(),
		"data_dir", cfg.DataDir, "price_venues", cfg.PriceVenues)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	flushCtx, stopFlush := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, run := range []func(context.Context, time.Duration){quotes.Run, priceRows.Run} {
		wg.Add(1)
		go func(run func(context.Context, time.Duration)) {
			defer wg.Done()
			run(flushCtx, cfg.FlushInterval)
		}(run)
	}

	col.Run(ctx) // returns after the in-flight round has handed over its rows

	log.Info("shutting down, flushing buffers", "event", "collector_stopping", "buffered_quotes", quotes.Buffered())
	stopFlush()
	wg.Wait()
	_, qerr := quotes.Flush()
	_, perr := priceRows.Flush()
	if qerr != nil || perr != nil {
		return fmt.Errorf("final flush: quotes=%v prices=%v", qerr, perr)
	}
	log.Info("collector stopped", "event", "collector_stopped")
	return nil
}

func buildSources(cfg config.Config, client *http.Client, log *slog.Logger) []collector.SourceConfig {
	var out []collector.SourceConfig
	if cfg.Sources["jupiter"] {
		if cfg.JupiterAPIKey == "" && cfg.JupiterOrderURL == config.DefaultJupiterOrderURL {
			// api.jup.ag rejects keyless requests; polling it would only log 401s.
			log.Warn("jupiter disabled: JUPITER_API_KEY is not set (get one at developers.jup.ag/portal), or point JUPITER_ORDER_URL at a mock",
				"event", "source_disabled", "source", "jupiter")
		} else {
			out = append(out, collector.SourceConfig{
				Source: &sources.Jupiter{
					OrderURL:    cfg.JupiterOrderURL,
					APIKey:      cfg.JupiterAPIKey,
					SlippageBps: cfg.SlippageBps,
					Client:      client,
				},
				RPS: cfg.JupiterRPS, Burst: cfg.JupiterBurst, Concurrency: 3,
			})
		}
	}
	if cfg.Sources["lifi"] {
		if cfg.LiFiAPIKey == "" && cfg.LiFiBaseURL == config.DefaultLiFiBaseURL {
			log.Warn("lifi has no API key: unauthenticated quotes are limited to 75 per 2 hours, so most LI.FI quotes will be skipped. Get a key at portal.li.fi",
				"event", "source_unauthenticated", "source", "lifi")
		}
		out = append(out, collector.SourceConfig{
			Source: &sources.LiFi{
				BaseURL:       cfg.LiFiBaseURL,
				APIKey:        cfg.LiFiAPIKey,
				Integrator:    cfg.LiFiIntegrator,
				SlippageBps:   cfg.SlippageBps,
				Order:         cfg.LiFiOrder,
				EVMAddress:    cfg.LiFiEVMAddress,
				SolanaAddress: cfg.LiFiSolanaAddress,
				Client:        client,
			},
			RPS: cfg.LiFiRPS, Burst: cfg.LiFiBurst, Concurrency: 4,
		})
	}
	return out
}

// warnBudgets compares each source's configured rate with what a round
// needs, so an undersized budget is obvious at startup instead of showing
// up later as skipped quotes.
func warnBudgets(cfg config.Config, srcs []collector.SourceConfig, log *slog.Logger) {
	for _, s := range srcs {
		jobs := 0
		for _, c := range cfg.Corridors {
			if s.Source.Supports(c) {
				jobs += len(cfg.SizeBuckets)
			}
		}
		if jobs == 0 {
			log.Warn("source supports none of the configured corridors", "event", "source_idle", "source", s.Source.Name())
			continue
		}
		need := float64(jobs) / cfg.PollInterval.Seconds()
		if need > s.RPS {
			pct := 100 * (1 - s.RPS/need)
			log.Warn("rate budget is below what each round needs; some quotes will be skipped",
				"event", "budget_insufficient", "source", s.Source.Name(),
				"quotes_per_round", jobs, "needed_rps", fmt.Sprintf("%.3f", need), "budget_rps", fmt.Sprintf("%.4f", s.RPS),
				"approx_skipped_pct", fmt.Sprintf("%.0f", pct),
				"hint", strings.ToUpper(s.Source.Name())+"_RPS, POLL_INTERVAL, SIZE_BUCKETS or CORRIDORS")
		}
	}
}
