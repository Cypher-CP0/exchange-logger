package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/rankings"
)

var now = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func latestJSON(computedAt time.Time) string {
	return `{
  "schema_version": 1,
  "computed_at": "` + computedAt.Format(time.RFC3339) + `",
  "data_through": "2026-10-23T11:59:30Z",
  "windows": ["24h", "7d"],
  "params": {"tie_bps": 2, "min_samples": 30, "availability_excludes": ["rate_limited", "unauthorized"]},
  "rankings": [
    {"window": "7d", "corridor": "base-usdc__sol-usdc", "size_bucket": 1000, "rounds": 900, "contested_rounds": 850,
     "routes": [
       {"source": "relay", "median_bps": 4.1, "p90_bps": 6, "win_rate": 0.7, "availability": 0.99, "attempts": 900, "samples": 891,
        "contested_rounds": 850, "median_seconds": 3, "median_fee_bps": 1, "low_sample": false, "last_ok": "2026-10-23T11:59:30Z",
        "top_routes": [{"label": "relay", "share": 1, "median_bps": 4.1}]},
       {"source": "lifi", "median_bps": 29.4, "p90_bps": 33, "win_rate": 0.3, "availability": 0.95, "attempts": 900, "samples": 855,
        "contested_rounds": 850, "median_seconds": 21, "median_fee_bps": 25, "low_sample": false, "last_ok": "2026-10-23T11:59:30Z",
        "top_routes": [{"label": "lifiIntents", "share": 0.8, "median_bps": 27.4}]}
     ]},
    {"window": "7d", "corridor": "base-usdc__sol-usdc", "size_bucket": 100, "rounds": 900, "contested_rounds": 0,
     "routes": [{"source": "lifi", "median_bps": 48.8, "p90_bps": 52, "win_rate": null, "availability": 1, "attempts": 900, "samples": 900,
        "contested_rounds": 0, "median_seconds": 1, "median_fee_bps": 48.5, "low_sample": false, "last_ok": null, "top_routes": []}]},
    {"window": "24h", "corridor": "base-usdc__sol-usdc", "size_bucket": 1000, "rounds": 120, "contested_rounds": 110,
     "routes": [
       {"source": "lifi", "median_bps": 28, "p90_bps": 30, "win_rate": 0.6, "availability": 0.5, "attempts": 100, "samples": 50,
        "contested_rounds": 110, "median_seconds": 21, "median_fee_bps": 25, "low_sample": false, "last_ok": null, "top_routes": []},
       {"source": "relay", "median_bps": 30, "p90_bps": 35, "win_rate": 0.4, "availability": 1, "attempts": 300, "samples": 300,
        "contested_rounds": 110, "median_seconds": 3, "median_fee_bps": 1, "low_sample": false, "last_ok": null, "top_routes": []}
     ]},
    {"window": "24h", "corridor": "sol-sol__sol-usdc", "size_bucket": 1000, "rounds": 120, "contested_rounds": 0,
     "routes": [{"source": "jupiter", "median_bps": 1.7, "p90_bps": 2.3, "win_rate": null, "availability": 1, "attempts": 100, "samples": 100,
        "contested_rounds": 0, "median_seconds": null, "median_fee_bps": 2, "low_sample": false, "last_ok": null, "top_routes": []}]}
  ]
}`
}

type fixture struct {
	dir    string
	store  *rankings.Store
	server *Server
}

func setup(t *testing.T, apiKey string, computedAt time.Time, writeLatest, writeStatus bool) *fixture {
	t.Helper()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	latest := filepath.Join(dir, "rankings", "latest.json")
	os.MkdirAll(filepath.Dir(latest), 0o755)
	if writeLatest {
		os.WriteFile(latest, []byte(latestJSON(computedAt)), 0o644)
	}
	statusPath := filepath.Join(dir, "status", "collector.json")
	if writeStatus {
		os.MkdirAll(filepath.Dir(statusPath), 0o755)
		os.WriteFile(statusPath, []byte(`{"collector_started_at":"2026-10-23T10:00:00Z","updated_at":"2026-10-23T11:59:45Z",
		  "last_round":{"poll_round":1,"started_at":"2026-10-23T11:59:44Z","skipped":false,"rows":21,"ok":21,
		  "sources":{"lifi":{"attempted":18,"ok":18},"jupiter":{"attempted":3,"ok":3}}}}`), 0o644)
	}
	store := rankings.NewStore(latest, log)
	store.Reload()
	s := New(Config{APIKey: apiKey, StaleAfter: 30 * time.Minute, CollectorStaleAfter: 2 * time.Minute, CollectorStatusPath: statusPath}, store, log)
	s.now = func() time.Time { return now }
	return &fixture{dir: dir, store: store, server: s}
}

func (f *fixture) get(t *testing.T, path string, headers ...string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	f.server.ServeHTTP(rec, req)
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("%s: response is not JSON: %s", path, rec.Body.String())
	}
	return rec.Code, body
}

func sources(body map[string]any) []string {
	var out []string
	for _, r := range body["routes"].([]any) {
		out = append(out, r.(map[string]any)["source"].(string))
	}
	return out
}

func TestRankingsLookup(t *testing.T) {
	f := setup(t, "", now.Add(-5*time.Minute), true, true)

	code, body := f.get(t, "/v1/rankings?from=base&to=solana&token=USDC&amount=500")
	if code != 200 || body["corridor"] != "base-usdc__sol-usdc" || body["size_bucket"] != 1000.0 || body["window"] != "7d" {
		t.Fatalf("%d %v", code, body)
	}
	if got := sources(body); len(got) != 2 || got[0] != "relay" || got[1] != "lifi" {
		t.Fatalf("routes must keep the ranker's order, got %v", got)
	}
	if body["stale"] != false || body["age_seconds"] != 300.0 || body["contested_rounds"] != 850.0 || body["requested_amount_usd"] != 500.0 {
		t.Fatalf("metadata %v", body)
	}

	// $150 is nearer $100 than $1,000 on a log scale.
	_, body = f.get(t, "/v1/rankings?from=base&to=sol&token=usdc&amount=150")
	if body["size_bucket"] != 100.0 || sources(body)[0] != "lifi" {
		t.Fatalf("$150 -> %v", body["size_bucket"])
	}

	// Other window, explicit corridor, explicit size.
	_, body = f.get(t, "/v1/rankings?corridor=base-usdc__sol-usdc&size_bucket=1000&window=24h")
	if body["window"] != "24h" || sources(body)[0] != "lifi" {
		t.Fatalf("24h: %v", body)
	}
}

func TestRankingsErrorsAndEmpty(t *testing.T) {
	f := setup(t, "", now.Add(-5*time.Minute), true, true)
	cases := []struct {
		path string
		code int
	}{
		{"/v1/rankings?from=base&to=solana&token=USDC", 400},             // no amount
		{"/v1/rankings?from=polygon&to=solana&token=USDC&amount=1", 400}, // unknown chain
		{"/v1/rankings?from=base&to=solana&amount=1", 400},               // no token
		{"/v1/rankings?from=base&to=solana&token=US%20DC&amount=1", 400}, // bad symbol
		{"/v1/rankings?from=base&to=solana&token=DAI&amount=100", 404},   // not tracked
		{"/v1/rankings?from=base&to=solana&token=USDC&amount=-5", 400},   // bad amount
		{"/v1/rankings?from=base&to=solana&token=USDC&amount=1&window=30d", 400},
		{"/v1/nope", 404},
	}
	for _, c := range cases {
		if code, body := f.get(t, c.path); code != c.code || body["error"] == nil {
			t.Errorf("%s: %d %v", c.path, code, body)
		}
	}

	// Tracked corridor with no data yet: 200, empty routes, a note telling FluxPay to query everyone.
	code, body := f.get(t, "/v1/rankings?from=arbitrum&to=solana&token=USDC&amount=1000")
	if code != 200 || len(body["routes"].([]any)) != 0 || body["note"] == nil {
		t.Fatalf("no-data corridor: %d %v", code, body)
	}
	// ETH -> USDC needs from_token/to_token.
	code, body = f.get(t, "/v1/rankings?from=base&from_token=ETH&to=solana&to_token=USDC&amount=1000")
	if code != 200 || body["corridor"] != "base-eth__sol-usdc" {
		t.Fatalf("eth corridor: %d %v", code, body)
	}
}

func TestStaleAndMissingRankings(t *testing.T) {
	f := setup(t, "", now.Add(-2*time.Hour), true, true)
	_, body := f.get(t, "/v1/rankings?from=base&to=solana&token=USDC&amount=1000")
	if body["stale"] != true || body["stale_reason"] != "ranker_behind" || body["note"] == nil {
		t.Fatalf("2h-old rankings must be stale: %v", body)
	}

	f = setup(t, "", now, false, true)
	if code, body := f.get(t, "/v1/rankings?from=base&to=solana&token=USDC&amount=1000"); code != 503 || body["error"] == nil {
		t.Fatalf("no latest.json: %d %v", code, body)
	}
}

func TestFreshRankingOfOldDataIsStale(t *testing.T) {
	// The ranker ran 5 minutes ago, but the collector stopped 16 hours ago.
	f := setup(t, "", now.Add(-5*time.Minute), true, true)
	path := filepath.Join(f.dir, "rankings", "latest.json")
	old := strings.Replace(latestJSON(now.Add(-5*time.Minute)), `"data_through": "2026-10-23T11:59:30Z"`, `"data_through": "2026-10-22T20:00:00Z"`, 1)
	os.WriteFile(path, []byte(old), 0o644)
	future := time.Now().Add(2 * time.Second)
	os.Chtimes(path, future, future)
	if _, err := f.store.Reload(); err != nil {
		t.Fatal(err)
	}
	_, body := f.get(t, "/v1/rankings?from=base&to=solana&token=USDC&amount=1000")
	if body["stale"] != true || body["stale_reason"] != "collector_behind" || body["data_through"] != "2026-10-22T20:00:00Z" {
		t.Fatalf("old data must be stale: %v", body)
	}
	_, body = f.get(t, "/v1/health")
	if body["status"] != "degraded" {
		t.Fatalf("health: %v", body)
	}
}

func TestAuth(t *testing.T) {
	key := "0123456789abcdef-test"
	f := setup(t, key, now, true, true)
	path := "/v1/rankings?from=base&to=solana&token=USDC&amount=1000"
	if code, _ := f.get(t, path); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	if code, _ := f.get(t, path, "X-API-Key", "wrong-key-wrong-key"); code != 401 {
		t.Fatalf("wrong key: %d", code)
	}
	if code, _ := f.get(t, path, "X-API-Key", key); code != 200 {
		t.Fatalf("x-api-key: %d", code)
	}
	if code, _ := f.get(t, path, "Authorization", "Bearer "+key); code != 200 {
		t.Fatalf("bearer: %d", code)
	}
	if code, _ := f.get(t, "/v1/health"); code != 200 {
		t.Fatalf("health must stay open: %d", code)
	}
}

func TestHealth(t *testing.T) {
	f := setup(t, "", now.Add(-5*time.Minute), true, true)
	_, body := f.get(t, "/v1/health")
	if body["status"] != "ok" || len(body["problems"].([]any)) != 0 {
		t.Fatalf("healthy: %v", body)
	}
	src := body["sources"].(map[string]any)
	lifi := src["lifi"].(map[string]any)
	if lifi["window"] != "24h" || lifi["attempts"] != 100.0 || lifi["availability"] != 0.5 {
		t.Fatalf("lifi availability: %v", lifi)
	}
	jup := src["jupiter"].(map[string]any)
	if jup["availability"] != 1.0 {
		t.Fatalf("jupiter: %v", jup)
	}

	f = setup(t, "", now.Add(-3*time.Hour), true, false)
	_, body = f.get(t, "/v1/health")
	if body["status"] != "degraded" || len(body["problems"].([]any)) != 2 {
		t.Fatalf("stale rankings + no collector: %v", body)
	}
}

func TestCorridors(t *testing.T) {
	f := setup(t, "", now, true, true)
	code, body := f.get(t, "/v1/corridors")
	if code != 200 {
		t.Fatal(code)
	}
	list := body["corridors"].([]any)
	if len(list) != 6 {
		t.Fatalf("want the 6 MVP corridors, got %d", len(list))
	}
	for _, c := range list {
		m := c.(map[string]any)
		if m["id"] == "base-usdc__sol-usdc" {
			sizes := m["sizes_with_data"].(map[string]any)["7d"].([]any)
			if len(sizes) != 2 || sizes[0] != 100.0 || m["from"].(map[string]any)["chain"] != "base" {
				t.Fatalf("base corridor: %v", m)
			}
		}
	}
}

func TestStoreReloadKeepsLastGoodCopy(t *testing.T) {
	f := setup(t, "", now, true, true)
	path := filepath.Join(f.dir, "rankings", "latest.json")
	first := f.store.Current().ComputedAt

	// A broken file must not replace a good snapshot.
	os.WriteFile(path, []byte(`{"schema_version": 1, "computed_at": `), 0o644)
	if _, err := f.store.Reload(); err == nil {
		t.Fatal("expected a parse error")
	}
	if !f.store.Current().ComputedAt.Equal(first) {
		t.Fatal("bad file replaced the good snapshot")
	}

	// A new good file is picked up.
	later := now.Add(10 * time.Minute)
	os.WriteFile(path, []byte(latestJSON(later)), 0o644)
	future := time.Now().Add(2 * time.Second)
	os.Chtimes(path, future, future)
	if changed, err := f.store.Reload(); err != nil || !changed || !f.store.Current().ComputedAt.Equal(later) {
		t.Fatalf("reload: changed=%v err=%v at=%v", changed, err, f.store.Current().ComputedAt)
	}

	// Unknown schema versions are rejected.
	if _, err := rankings.Parse([]byte(`{"schema_version": 2, "computed_at": "2026-10-23T12:00:00Z"}`)); err == nil {
		t.Fatal("schema_version 2 must be rejected")
	}
}

func TestNearestSize(t *testing.T) {
	sizes := []int{100, 1000, 10000}
	for amount, want := range map[float64]int{1: 100, 150: 100, 316: 100, 317: 1000, 500: 1000, 3000: 1000, 4000: 10000, 1e6: 10000} {
		if got := rankings.NearestSize(amount, sizes); got != want {
			t.Errorf("NearestSize(%v) = %d, want %d", amount, got, want)
		}
	}
}