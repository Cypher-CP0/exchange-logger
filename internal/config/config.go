// Package config reads collector settings from the environment, with an
// optional .env file for local runs. Real environment variables win over
// the file.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
)

// Defaults for the public endpoints.
const (
	DefaultJupiterOrderURL = "https://api.jup.ag/swap/v2/order"
	DefaultLiFiBaseURL     = "https://li.quest"
	DefaultBinanceURL      = "https://api.binance.com"
	DefaultOKXURL          = "https://www.okx.com"

	// Quote-only addresses. LI.FI requires a sender address even for a quote;
	// nothing is ever signed or sent from these. The EVM one appears in LI.FI's
	// own docs examples; the Solana one is a freshly generated public key whose
	// private key was discarded.
	DefaultLiFiEVMAddress    = "0x552008c0f6870c2f77e5cC1d2eb9bdff03e30Ea0"
	DefaultLiFiSolanaAddress = "FxyE69TEovt8MTRfB1DnsFUpQcLnUFZqDJBM1UBw2Rzd"

	// LI.FI unauthenticated /v1/quote limit: 75 requests per 2 hours.
	lifiUnauthRPS = 75.0 / 7200.0
	// LI.FI keyed default: 100 per minute; stay at 90% of it.
	lifiKeyedRPS = 1.5
)

// Config is everything the collector needs.
type Config struct {
	DataDir        string
	PollInterval   time.Duration
	RequestTimeout time.Duration
	FlushInterval  time.Duration
	FlushMaxRows   int
	SizeBuckets    []int32
	Corridors      []model.Corridor
	SlippageBps    int
	PauseAfter     int
	PauseFor       time.Duration
	LogLevel       slog.Level

	Sources map[string]bool // enabled sources

	JupiterOrderURL string
	JupiterAPIKey   string
	JupiterRPS      float64
	JupiterBurst    int

	LiFiBaseURL       string
	LiFiAPIKey        string
	LiFiIntegrator    string
	LiFiOrder         string
	LiFiEVMAddress    string
	LiFiSolanaAddress string
	LiFiRPS           float64
	LiFiBurst         int

	PriceVenues []string
	BinanceURL  string
	OKXURL      string
}

// LoadDotEnv sets variables from a .env file that aren't already set.
// A missing file is not an error.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		} else if i := strings.Index(v, " #"); i >= 0 {
			v = strings.TrimSpace(v[:i]) // inline comment on an unquoted value
		}
		if _, set := os.LookupEnv(k); !set {
			os.Setenv(k, v)
		}
	}
	return sc.Err()
}

// Load reads the environment.
func Load() (Config, error) {
	var errs []error
	e := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	c := Config{
		DataDir:           str("DATA_DIR", "./data"),
		SlippageBps:       0,
		JupiterOrderURL:   str("JUPITER_ORDER_URL", DefaultJupiterOrderURL),
		JupiterAPIKey:     os.Getenv("JUPITER_API_KEY"),
		LiFiBaseURL:       str("LIFI_BASE_URL", DefaultLiFiBaseURL),
		LiFiAPIKey:        os.Getenv("LIFI_API_KEY"),
		LiFiIntegrator:    os.Getenv("LIFI_INTEGRATOR"),
		LiFiOrder:         strings.ToUpper(str("LIFI_ORDER", "CHEAPEST")),
		LiFiEVMAddress:    str("LIFI_EVM_ADDRESS", DefaultLiFiEVMAddress),
		LiFiSolanaAddress: str("LIFI_SOLANA_ADDRESS", DefaultLiFiSolanaAddress),
		BinanceURL:        str("BINANCE_BASE_URL", DefaultBinanceURL),
		OKXURL:            str("OKX_BASE_URL", DefaultOKXURL),
		PriceVenues:       list("PRICE_VENUES", "binance,okx"),
	}
	var err error
	c.PollInterval, err = dur("POLL_INTERVAL", 30*time.Second)
	e(err)
	c.RequestTimeout, err = dur("REQUEST_TIMEOUT", 5*time.Second)
	e(err)
	c.FlushInterval, err = dur("FLUSH_INTERVAL", 5*time.Minute)
	e(err)
	c.PauseFor, err = dur("PAUSE_DURATION", 5*time.Minute)
	e(err)
	c.FlushMaxRows, err = integer("FLUSH_MAX_ROWS", 50000)
	e(err)
	c.SlippageBps, err = integer("SLIPPAGE_BPS", 50)
	e(err)
	c.PauseAfter, err = integer("PAUSE_AFTER_ROUNDS", 10)
	e(err)
	c.JupiterBurst, err = integer("JUPITER_BURST", 3)
	e(err)
	c.JupiterRPS, err = float("JUPITER_RPS", 1)
	e(err)

	// LI.FI's budget depends on whether there is a key.
	defRPS, defBurst := lifiUnauthRPS, 2
	if c.LiFiAPIKey != "" {
		defRPS, defBurst = lifiKeyedRPS, 5
	}
	c.LiFiRPS, err = float("LIFI_RPS", defRPS)
	e(err)
	c.LiFiBurst, err = integer("LIFI_BURST", defBurst)
	e(err)

	for _, s := range list("SIZE_BUCKETS", "100,1000,10000") {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("SIZE_BUCKETS: %q is not a positive integer", s))
			continue
		}
		c.SizeBuckets = append(c.SizeBuckets, int32(n))
	}
	c.Corridors, err = model.SelectCorridors(list("CORRIDORS", ""))
	e(err)

	c.Sources = map[string]bool{}
	for _, s := range list("SOURCES", "jupiter,lifi") {
		switch s {
		case "jupiter", "lifi":
			c.Sources[s] = true
		default:
			errs = append(errs, fmt.Errorf("SOURCES: unknown source %q (known: jupiter, lifi)", s))
		}
	}
	for _, v := range c.PriceVenues {
		if v != "binance" && v != "okx" {
			errs = append(errs, fmt.Errorf("PRICE_VENUES: unknown venue %q (known: binance, okx)", v))
		}
	}
	if c.LiFiOrder != "CHEAPEST" && c.LiFiOrder != "FASTEST" {
		errs = append(errs, fmt.Errorf("LIFI_ORDER must be CHEAPEST or FASTEST"))
	}
	if c.SlippageBps < 0 || c.SlippageBps > 10000 {
		errs = append(errs, fmt.Errorf("SLIPPAGE_BPS must be between 0 and 10000"))
	}
	if c.RequestTimeout >= c.PollInterval {
		errs = append(errs, fmt.Errorf("REQUEST_TIMEOUT (%s) must be shorter than POLL_INTERVAL (%s)", c.RequestTimeout, c.PollInterval))
	}
	if err := c.LogLevel.UnmarshalText([]byte(str("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	return c, errors.Join(errs...)
}

func str(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func list(key, def string) []string {
	raw := str(key, def)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.ToLower(p))
		}
	}
	return out
}

func dur(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def, fmt.Errorf("%s: %q is not a positive duration like 30s or 5m", key, v)
	}
	return d, nil
}

func integer(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def, fmt.Errorf("%s: %q is not an integer", key, v)
	}
	return n, nil
}

func float(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return def, fmt.Errorf("%s: %q is not a positive number", key, v)
	}
	return f, nil
}

// API is the rankings API's configuration.
type API struct {
	DataDir             string
	ListenAddr          string
	APIKey              string
	AllowNoAuth         bool
	StaleAfter          time.Duration // rankings older than this are flagged stale
	CollectorStaleAfter time.Duration // collector status older than this is flagged stale
	ReloadInterval      time.Duration
	LogLevel            slog.Level
}

// LoadAPI reads the API's environment.
func LoadAPI() (API, error) {
	var errs []error
	e := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	c := API{
		DataDir:    str("DATA_DIR", "./data"),
		ListenAddr: str("API_LISTEN_ADDR", "127.0.0.1:8081"),
		APIKey:     os.Getenv("API_KEY"),
	}
	var err error
	c.StaleAfter, err = dur("RANKINGS_STALE_AFTER", 30*time.Minute)
	e(err)
	c.CollectorStaleAfter, err = dur("COLLECTOR_STALE_AFTER", 2*time.Minute)
	e(err)
	c.ReloadInterval, err = dur("RANKINGS_RELOAD_INTERVAL", 15*time.Second)
	e(err)
	switch strings.ToLower(str("API_ALLOW_NO_AUTH", "false")) {
	case "true", "1", "yes":
		c.AllowNoAuth = true
	case "false", "0", "no":
	default:
		errs = append(errs, fmt.Errorf("API_ALLOW_NO_AUTH must be true or false"))
	}
	if c.APIKey != "" && len(c.APIKey) < 16 {
		errs = append(errs, fmt.Errorf("API_KEY must be at least 16 characters"))
	}
	if err := c.LogLevel.UnmarshalText([]byte(str("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	return c, errors.Join(errs...)
}
