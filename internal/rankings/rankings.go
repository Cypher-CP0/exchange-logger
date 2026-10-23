// Package rankings loads the ranker's latest.json and keeps it in memory, so
// the API answers from RAM and never scans Parquet per request.
package rankings

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

// SchemaVersion is the latest.json format this package understands.
const SchemaVersion = 1

// File is latest.json as written by ranker/rank.py.
type File struct {
	SchemaVersion int        `json:"schema_version"`
	ComputedAt    time.Time  `json:"computed_at"`
	DataThrough   *time.Time `json:"data_through"`
	Windows       []string   `json:"windows"`
	Params        Params     `json:"params"`
	Rankings      []Group    `json:"rankings"`
}

// Params are the ranker settings that shaped the numbers.
type Params struct {
	TieBps               float64  `json:"tie_bps"`
	MinSamples           int      `json:"min_samples"`
	AvailabilityExcludes []string `json:"availability_excludes"`
}

// Group is one window x corridor x size bucket.
type Group struct {
	Window          string  `json:"window"`
	Corridor        string  `json:"corridor"`
	SizeBucket      int     `json:"size_bucket"`
	Rounds          int     `json:"rounds"`
	ContestedRounds int     `json:"contested_rounds"`
	Routes          []Route `json:"routes"`
}

// Route is one source's record in a group, already in rank order.
type Route struct {
	Source          string       `json:"source"`
	MedianBps       *float64     `json:"median_bps"`
	P90Bps          *float64     `json:"p90_bps"`
	WinRate         *float64     `json:"win_rate"`
	Availability    *float64     `json:"availability"`
	Attempts        int          `json:"attempts"`
	Samples         int          `json:"samples"`
	ContestedRounds int          `json:"contested_rounds"`
	MedianSeconds   *float64     `json:"median_seconds"`
	MedianFeeBps    *float64     `json:"median_fee_bps"`
	LowSample       bool         `json:"low_sample"`
	LastOK          *time.Time   `json:"last_ok"`
	TopRoutes       []RouteLabel `json:"top_routes"`
}

// RouteLabel is a bridge or DEX a source used, with its share of samples.
type RouteLabel struct {
	Label     *string  `json:"label"`
	Share     *float64 `json:"share"`
	MedianBps *float64 `json:"median_bps"`
}

type key struct {
	window   string
	corridor string
	size     int
}

// Snapshot is an immutable, indexed copy of one latest.json.
type Snapshot struct {
	File
	LoadedAt time.Time
	byKey    map[key]*Group
	sizes    map[[2]string][]int // (window, corridor) -> sorted size buckets
}

// Group returns the ranking for a window, corridor and size, if any.
func (s *Snapshot) Group(window, corridor string, size int) (*Group, bool) {
	g, ok := s.byKey[key{window, corridor, size}]
	return g, ok
}

// Sizes returns the size buckets ranked for a corridor in a window.
func (s *Snapshot) Sizes(window, corridor string) []int {
	return s.sizes[[2]string{window, corridor}]
}

// HasWindow reports whether the ranker computed this window.
func (s *Snapshot) HasWindow(w string) bool {
	for _, x := range s.Windows {
		if x == w {
			return true
		}
	}
	return false
}

// Corridors returns every corridor that has at least one ranking.
func (s *Snapshot) Corridors() []string {
	seen := map[string]bool{}
	var out []string
	for _, g := range s.Rankings {
		if !seen[g.Corridor] {
			seen[g.Corridor] = true
			out = append(out, g.Corridor)
		}
	}
	sort.Strings(out)
	return out
}

// Parse validates and indexes a latest.json document.
func Parse(b []byte) (*Snapshot, error) {
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("decode latest.json: %w", err)
	}
	if f.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("latest.json schema_version %d, this API understands %d", f.SchemaVersion, SchemaVersion)
	}
	if f.ComputedAt.IsZero() {
		return nil, fmt.Errorf("latest.json has no computed_at")
	}
	s := &Snapshot{File: f, byKey: map[key]*Group{}, sizes: map[[2]string][]int{}}
	for i := range s.Rankings {
		g := &s.Rankings[i]
		s.byKey[key{g.Window, g.Corridor, g.SizeBucket}] = g
		k := [2]string{g.Window, g.Corridor}
		s.sizes[k] = append(s.sizes[k], g.SizeBucket)
	}
	for k := range s.sizes {
		sort.Ints(s.sizes[k])
	}
	return s, nil
}

// Store holds the current snapshot and reloads it when the file changes.
// A bad or half-written file never replaces a good snapshot.
type Store struct {
	path string
	log  *slog.Logger
	cur  atomic.Pointer[Snapshot]

	lastMod  time.Time
	lastSize int64
}

// NewStore creates a store for path (usually data/rankings/latest.json).
func NewStore(path string, log *slog.Logger) *Store { return &Store{path: path, log: log} }

// Current returns the loaded snapshot, or nil before the first good load.
func (s *Store) Current() *Snapshot { return s.cur.Load() }

// Reload reads the file if it changed since the last load.
func (s *Store) Reload() (changed bool, err error) {
	st, err := os.Stat(s.path)
	if err != nil {
		return false, err
	}
	if s.cur.Load() != nil && st.ModTime().Equal(s.lastMod) && st.Size() == s.lastSize {
		return false, nil
	}
	b, err := os.ReadFile(s.path)
	if err != nil {
		return false, err
	}
	snap, err := Parse(b)
	if err != nil {
		return false, err
	}
	snap.LoadedAt = time.Now()
	s.cur.Store(snap)
	s.lastMod, s.lastSize = st.ModTime(), st.Size()
	return true, nil
}

// Run reloads every interval until ctx is done.
func (s *Store) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var lastErr string
	for {
		changed, err := s.Reload()
		switch {
		case err != nil:
			// Log each distinct error once, not every tick.
			if err.Error() != lastErr {
				s.log.Warn("cannot load rankings; serving the previous copy if any", "event", "rankings_load_failed", "path", s.path, "error", err.Error())
				lastErr = err.Error()
			}
		case changed:
			lastErr = ""
			snap := s.Current()
			s.log.Info("rankings loaded", "event", "rankings_loaded", "computed_at", snap.ComputedAt.UTC().Format(time.RFC3339), "groups", len(snap.Rankings))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// NearestSize picks the bucket closest to amount on a log scale: fixed fees
// make cost depend on order of magnitude, so $500 is closer to $1,000 than
// to $100.
func NearestSize(amount float64, sizes []int) int {
	best, bestD := 0, math.Inf(1)
	for _, s := range sizes {
		if s <= 0 {
			continue
		}
		d := math.Abs(math.Log(amount) - math.Log(float64(s)))
		if d < bestD || (d == bestD && s > best) {
			best, bestD = s, d
		}
	}
	return best
}
