// Package api serves the ranker's output to FluxPay over HTTP.
//
//	GET /v1/rankings   ranked sources for one corridor and amount
//	GET /v1/corridors  what is tracked and which sizes have data
//	GET /v1/health     rankings age, collector freshness, per-source availability
//
// Everything is answered from memory (the rankings.Store snapshot plus a tiny
// status file); no request ever scans Parquet.
package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/rankings"
)

// Config controls staleness thresholds and auth.
type Config struct {
	APIKey              string        // empty disables auth
	StaleAfter          time.Duration // rankings older than this are "stale"
	CollectorStaleAfter time.Duration
	CollectorStatusPath string // data/status/collector.json
	DefaultSizes        []int  // used when a corridor has no ranked sizes yet
}

// Server is the HTTP handler.
type Server struct {
	cfg   Config
	store *rankings.Store
	log   *slog.Logger
	now   func() time.Time
	mux   *http.ServeMux
}

// New builds the server and its routes.
func New(cfg Config, store *rankings.Store, log *slog.Logger) *Server {
	if len(cfg.DefaultSizes) == 0 {
		cfg.DefaultSizes = []int{100, 1000, 10000}
	}
	s := &Server{cfg: cfg, store: store, log: log, now: time.Now, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /v1/rankings", s.handleRankings)
	s.mux.HandleFunc("GET /v1/corridors", s.handleCorridors)
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found", nil)
	})
	return s
}

// ServeHTTP applies recovery, logging and auth around the routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := s.now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		if p := recover(); p != nil {
			s.log.Error("panic serving request", "event", "api_panic", "path", r.URL.Path, "panic", fmt.Sprint(p))
			writeError(rec, http.StatusInternalServerError, "internal error", nil)
		}
		level := slog.LevelInfo
		if r.URL.Path == "/v1/health" {
			level = slog.LevelDebug
		}
		s.log.Log(r.Context(), level, "request", "event", "api_request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", s.now().Sub(start).Milliseconds())
	}()
	if s.cfg.APIKey != "" && r.URL.Path != "/v1/health" && !s.authorized(r) {
		writeError(rec, http.StatusUnauthorized, "missing or invalid API key", map[string]any{"hint": "send X-API-Key or Authorization: Bearer"})
		return
	}
	s.mux.ServeHTTP(rec, r)
}

func (s *Server) authorized(r *http.Request) bool {
	got := r.Header.Get("X-API-Key")
	if got == "" {
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			got = strings.TrimPrefix(h, "Bearer ")
		}
	}
	return got != "" && subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.APIKey)) == 1
}

// ---------------------------------------------------------------- rankings

// RankingsResponse is the body of GET /v1/rankings.
type RankingsResponse struct {
	Corridor           string           `json:"corridor"`
	SizeBucket         int              `json:"size_bucket"`
	RequestedAmountUSD *float64         `json:"requested_amount_usd,omitempty"`
	Window             string           `json:"window"`
	ComputedAt         time.Time        `json:"computed_at"`
	AgeSeconds         int64            `json:"age_seconds"`
	DataThrough        *time.Time       `json:"data_through"` // newest quote the ranking saw
	Stale              bool             `json:"stale"`
	StaleReason        string           `json:"stale_reason,omitempty"` // ranker_behind | collector_behind | no_data
	Rounds             int              `json:"rounds"`
	ContestedRounds    int              `json:"contested_rounds"`
	TieBps             float64          `json:"tie_bps"`
	Routes             []rankings.Route `json:"routes"`
	Note               string           `json:"note,omitempty"`
}

var symbolRE = regexp.MustCompile(`^[A-Za-z0-9]{1,12}$`)

func (s *Server) handleRankings(w http.ResponseWriter, r *http.Request) {
	snap := s.store.Current()
	if snap == nil {
		writeError(w, http.StatusServiceUnavailable, "rankings not available yet", map[string]any{"hint": "is the ranker running? query every source until it is"})
		return
	}
	q := r.URL.Query()

	corridor, err := resolveCorridor(q.Get("corridor"), q.Get("from"), q.Get("to"), q.Get("token"), q.Get("from_token"), q.Get("to_token"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), nil)
		return
	}
	tracked := s.tracked(snap)
	if !tracked[corridor] {
		writeError(w, http.StatusNotFound, "corridor not tracked", map[string]any{"corridor": corridor, "tracked": sortedKeys(tracked)})
		return
	}

	window := q.Get("window")
	if window == "" {
		window = defaultWindow(snap)
	}
	if !snap.HasWindow(window) {
		writeError(w, http.StatusBadRequest, "unknown window", map[string]any{"window": window, "windows": snap.Windows})
		return
	}

	sizes := snap.Sizes(window, corridor)
	if len(sizes) == 0 {
		sizes = s.cfg.DefaultSizes
	}
	resp := RankingsResponse{Corridor: corridor, Window: window, TieBps: snap.Params.TieBps, Routes: []rankings.Route{}}
	switch {
	case q.Get("size_bucket") != "":
		n, err := strconv.Atoi(q.Get("size_bucket"))
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "size_bucket must be a positive integer", nil)
			return
		}
		resp.SizeBucket = n
	case q.Get("amount") != "":
		amt, err := strconv.ParseFloat(q.Get("amount"), 64)
		if err != nil || amt <= 0 || math.IsInf(amt, 0) || math.IsNaN(amt) {
			writeError(w, http.StatusBadRequest, "amount must be a positive number of USD", nil)
			return
		}
		resp.RequestedAmountUSD = &amt
		resp.SizeBucket = rankings.NearestSize(amt, sizes)
	default:
		writeError(w, http.StatusBadRequest, "amount (USD) or size_bucket is required", nil)
		return
	}

	now := s.now()
	resp.ComputedAt = snap.ComputedAt.UTC()
	resp.AgeSeconds = int64(now.Sub(snap.ComputedAt).Seconds())
	resp.DataThrough = snap.DataThrough
	resp.StaleReason, _ = s.staleness(snap, now)
	resp.Stale = resp.StaleReason != ""

	if g, ok := snap.Group(window, corridor, resp.SizeBucket); ok {
		resp.Rounds, resp.ContestedRounds, resp.Routes = g.Rounds, g.ContestedRounds, g.Routes
	} else {
		resp.Note = "no data yet for this corridor and size; query every source"
	}
	if resp.Stale {
		_, why := s.staleness(snap, now)
		resp.Note = strings.TrimSpace(resp.Note + " rankings are stale (" + why + "); prefer querying every source")
	}
	writeJSON(w, http.StatusOK, resp)
}

// staleness says whether rankings can be trusted. A ranking computed a
// minute ago is still stale if the newest quote in it is hours old, which
// is what happens when the collector stops but the ranker keeps running.
func (s *Server) staleness(snap *rankings.Snapshot, now time.Time) (reason, explanation string) {
	if age := now.Sub(snap.ComputedAt); age > s.cfg.StaleAfter {
		return "ranker_behind", fmt.Sprintf("rankings computed %s ago", age.Round(time.Minute))
	}
	if snap.DataThrough == nil {
		return "no_data", "no quotes collected yet"
	}
	if age := now.Sub(*snap.DataThrough); age > s.cfg.StaleAfter {
		return "collector_behind", fmt.Sprintf("newest quote is %s old", age.Round(time.Minute))
	}
	return "", ""
}

// resolveCorridor accepts either corridor=<id> or from/to chains with a
// token (both sides) or from_token/to_token.
func resolveCorridor(id, from, to, token, fromToken, toToken string) (string, error) {
	if id != "" {
		id = strings.ToLower(strings.TrimSpace(id))
		if !strings.Contains(id, "__") {
			return "", fmt.Errorf("corridor must look like base-usdc__sol-usdc")
		}
		return id, nil
	}
	if from == "" || to == "" {
		return "", fmt.Errorf("give corridor=<id>, or from and to chains (e.g. from=base&to=solana&token=USDC)")
	}
	fc, ok := model.ChainByName(from)
	if !ok {
		return "", fmt.Errorf("unknown chain %q (known: solana, base, arbitrum, ethereum)", from)
	}
	tc, ok := model.ChainByName(to)
	if !ok {
		return "", fmt.Errorf("unknown chain %q (known: solana, base, arbitrum, ethereum)", to)
	}
	if fromToken == "" {
		fromToken = token
	}
	if toToken == "" {
		toToken = token
	}
	if fromToken == "" || toToken == "" {
		return "", fmt.Errorf("give token (same on both sides) or from_token and to_token")
	}
	if !symbolRE.MatchString(fromToken) || !symbolRE.MatchString(toToken) {
		return "", fmt.Errorf("token symbols must be 1-12 letters or digits")
	}
	return model.CorridorID(fc.Key, fromToken, tc.Key, toToken), nil
}

func defaultWindow(snap *rankings.Snapshot) string {
	if snap.HasWindow("7d") {
		return "7d"
	}
	if len(snap.Windows) > 0 {
		return snap.Windows[len(snap.Windows)-1]
	}
	return "7d"
}

// tracked is every corridor the collector is built for plus anything the
// ranker has data for.
func (s *Server) tracked(snap *rankings.Snapshot) map[string]bool {
	out := map[string]bool{}
	for _, c := range model.DefaultCorridors() {
		out[c.ID] = true
	}
	if snap != nil {
		for _, c := range snap.Corridors() {
			out[c] = true
		}
	}
	return out
}

// --------------------------------------------------------------- corridors

type corridorInfo struct {
	ID        string           `json:"id"`
	From      endpoint         `json:"from"`
	To        endpoint         `json:"to"`
	SameChain bool             `json:"same_chain"`
	Sizes     map[string][]int `json:"sizes_with_data"` // window -> buckets
}

type endpoint struct {
	Chain string `json:"chain"`
	Token string `json:"token"`
}

func (s *Server) handleCorridors(w http.ResponseWriter, r *http.Request) {
	snap := s.store.Current()
	byID := map[string]model.Corridor{}
	for _, c := range model.DefaultCorridors() {
		byID[c.ID] = c
	}
	var out []corridorInfo
	for _, id := range sortedKeys(s.tracked(snap)) {
		ci := corridorInfo{ID: id, Sizes: map[string][]int{}}
		if c, ok := byID[id]; ok {
			ci.From = endpoint{c.From.Chain.Name, c.From.Symbol}
			ci.To = endpoint{c.To.Chain.Name, c.To.Symbol}
			ci.SameChain = c.SameChain()
		} else if f, t, ok := parseCorridorID(id); ok {
			ci.From, ci.To, ci.SameChain = f, t, f.Chain == t.Chain
		}
		if snap != nil {
			for _, win := range snap.Windows {
				ci.Sizes[win] = append([]int{}, snap.Sizes(win, id)...)
			}
		}
		out = append(out, ci)
	}
	resp := map[string]any{"corridors": out, "size_buckets": s.cfg.DefaultSizes}
	if snap != nil {
		resp["windows"] = snap.Windows
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseCorridorID(id string) (endpoint, endpoint, bool) {
	a, b, ok := strings.Cut(id, "__")
	if !ok {
		return endpoint{}, endpoint{}, false
	}
	fc, ft, ok1 := strings.Cut(a, "-")
	tc, tt, ok2 := strings.Cut(b, "-")
	if !ok1 || !ok2 {
		return endpoint{}, endpoint{}, false
	}
	name := func(k string) string {
		if c, ok := model.ChainByName(k); ok {
			return c.Name
		}
		return k
	}
	return endpoint{name(fc), strings.ToUpper(ft)}, endpoint{name(tc), strings.ToUpper(tt)}, true
}

// ------------------------------------------------------------------ health

type collectorStatus struct {
	StartedAt time.Time `json:"collector_started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	LastRound struct {
		PollRound  int64     `json:"poll_round"`
		StartedAt  time.Time `json:"started_at"`
		Skipped    bool      `json:"skipped"`
		SkipReason string    `json:"skip_reason"`
		Rows       int       `json:"rows"`
		OK         int       `json:"ok"`
		Sources    map[string]struct {
			Attempted int            `json:"attempted"`
			OK        int            `json:"ok"`
			Errors    map[string]int `json:"errors"`
			Paused    bool           `json:"paused"`
		} `json:"sources"`
	} `json:"last_round"`
}

type sourceHealth struct {
	Availability *float64 `json:"availability"`
	Attempts     int      `json:"attempts"`
	Window       string   `json:"window"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	now := s.now()
	var problems []string
	resp := map[string]any{"time": now.UTC()}

	snap := s.store.Current()
	rk := map[string]any{"loaded": snap != nil}
	if snap == nil {
		problems = append(problems, "rankings not loaded (ranker not run yet?)")
	} else {
		reason, why := s.staleness(snap, now)
		rk["computed_at"] = snap.ComputedAt.UTC()
		rk["data_through"] = snap.DataThrough
		rk["age_seconds"] = int64(now.Sub(snap.ComputedAt).Seconds())
		rk["stale"] = reason != ""
		if reason != "" {
			rk["stale_reason"] = reason
			problems = append(problems, "rankings stale: "+why)
		}
		resp["sources"] = sourceAvailability(snap)
	}
	resp["rankings"] = rk

	col := map[string]any{"status_file": false}
	if b, err := os.ReadFile(s.cfg.CollectorStatusPath); err != nil {
		problems = append(problems, "collector status not found (collector not running?)")
	} else {
		var st collectorStatus
		if err := json.Unmarshal(b, &st); err != nil {
			problems = append(problems, "collector status unreadable")
		} else {
			age := now.Sub(st.UpdatedAt)
			col["status_file"] = true
			col["updated_at"] = st.UpdatedAt.UTC()
			col["age_seconds"] = int64(age.Seconds())
			col["stale"] = age > s.cfg.CollectorStaleAfter
			col["last_round"] = st.LastRound
			if age > s.cfg.CollectorStaleAfter {
				problems = append(problems, fmt.Sprintf("collector last reported %s ago", age.Round(time.Second)))
			}
			if st.LastRound.Skipped {
				problems = append(problems, "collector skipped its last round: "+st.LastRound.SkipReason)
			}
			for name, src := range st.LastRound.Sources {
				if src.Paused {
					problems = append(problems, "source paused: "+name)
				}
			}
		}
	}
	resp["collector"] = col

	sort.Strings(problems)
	resp["status"] = "ok"
	if len(problems) > 0 {
		resp["status"] = "degraded"
	}
	resp["problems"] = append([]string{}, problems...)
	writeJSON(w, http.StatusOK, resp)
}

// sourceAvailability is attempts-weighted availability per source over the
// shortest window, across all corridors and sizes.
func sourceAvailability(snap *rankings.Snapshot) map[string]sourceHealth {
	window := ""
	if snap.HasWindow("24h") {
		window = "24h"
	} else if len(snap.Windows) > 0 {
		window = snap.Windows[0]
	}
	type acc struct{ ok, n float64 }
	sums := map[string]*acc{}
	for _, g := range snap.Rankings {
		if g.Window != window {
			continue
		}
		for _, rt := range g.Routes {
			a := sums[rt.Source]
			if a == nil {
				a = &acc{}
				sums[rt.Source] = a
			}
			if rt.Availability != nil {
				a.ok += *rt.Availability * float64(rt.Attempts)
			}
			a.n += float64(rt.Attempts)
		}
	}
	out := map[string]sourceHealth{}
	for name, a := range sums {
		h := sourceHealth{Attempts: int(a.n), Window: window}
		if a.n > 0 {
			v := math.Round(a.ok/a.n*1000) / 1000
			h.Availability = &v
		}
		out[name] = h
	}
	return out
}

// ----------------------------------------------------------------- helpers

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string, extra map[string]any) {
	body := map[string]any{"error": msg}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, body)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}