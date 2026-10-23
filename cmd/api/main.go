// Command api serves route rankings to FluxPay. It reads the ranker's
// data/rankings/latest.json into memory, reloads it when it changes, and
// never touches the raw Parquet files.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/api"
	"github.com/Cypher-CP0/exchange-logger/internal/config"
	"github.com/Cypher-CP0/exchange-logger/internal/rankings"
)

func main() {
	if err := config.LoadDotEnv(".env"); err != nil {
		fmt.Fprintln(os.Stderr, "reading .env:", err)
		os.Exit(1)
	}
	cfg, err := config.LoadAPI()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:\n"+err.Error())
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "exchange-logger-api")

	if cfg.APIKey == "" {
		if !isLoopback(cfg.ListenAddr) && !cfg.AllowNoAuth {
			log.Error("refusing to listen on a non-loopback address without API_KEY (set API_KEY, or API_ALLOW_NO_AUTH=true on a private network)",
				"event", "api_insecure_config", "listen_addr", cfg.ListenAddr)
			os.Exit(2)
		}
		log.Warn("API_KEY is not set; requests are not authenticated", "event", "api_no_auth", "listen_addr", cfg.ListenAddr)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := rankings.NewStore(filepath.Join(cfg.DataDir, "rankings", "latest.json"), log)
	go store.Run(ctx, cfg.ReloadInterval)

	srv := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: api.New(api.Config{
			APIKey:              cfg.APIKey,
			StaleAfter:          cfg.StaleAfter,
			CollectorStaleAfter: cfg.CollectorStaleAfter,
			CollectorStatusPath: filepath.Join(cfg.DataDir, "status", "collector.json"),
		}, store, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "event", "api_started", "listen_addr", cfg.ListenAddr, "data_dir", cfg.DataDir, "auth", cfg.APIKey != "")
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("api stopped", "event", "api_failed", "error", err.Error())
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("shutdown", "event", "api_shutdown_failed", "error", err.Error())
		}
		log.Info("api stopped", "event", "api_stopped")
	}
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
