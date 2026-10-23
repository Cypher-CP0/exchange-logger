package sink

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestFlushWritesOneFilePerPartitionAndReadsBack(t *testing.T) {
	root := t.TempDir()
	s := NewPartitioned(root, func(q model.Quote) string {
		return DatePartition(q.TS) + "/corridor=" + q.Corridor
	}, 1000, quietLog())

	ts := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	amt, _ := model.DecimalFromBig(model.ToUnits(1000, 6))
	s.Add(
		model.Quote{QuoteID: "a", TS: ts, Source: "lifi", Corridor: "base-usdc__sol-usdc", SizeBucket: 1000, InputAmount: amt, QuoteOK: true, CostBps: model.Ptr(12.5)},
		model.Quote{QuoteID: "b", TS: ts, Source: "lifi", Corridor: "base-usdc__sol-usdc", SizeBucket: 100, InputAmount: amt, ErrorCode: "timeout"},
		model.Quote{QuoteID: "c", TS: ts.Add(24 * time.Hour), Source: "jupiter", Corridor: "sol-sol__sol-usdc", SizeBucket: 100, InputAmount: amt, QuoteOK: true},
	)
	if s.Buffered() != 3 {
		t.Fatalf("buffered %d", s.Buffered())
	}
	files, err := s.Flush()
	if err != nil || files != 2 {
		t.Fatalf("flush: files=%d err=%v", files, err)
	}
	if s.Buffered() != 0 {
		t.Fatal("buffer not drained")
	}

	matches, _ := filepath.Glob(filepath.Join(root, "dt=2026-10-22", "corridor=base-usdc__sol-usdc", "part-*.parquet"))
	if len(matches) != 1 {
		t.Fatalf("files in partition: %v", matches)
	}
	var leftovers []string
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if strings.HasSuffix(p, ".tmp") {
			leftovers = append(leftovers, p)
		}
		return nil
	})
	if len(leftovers) > 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}

	rows, err := parquet.ReadFile[model.Quote](matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].QuoteID != "a" || *rows[0].CostBps != 12.5 || rows[1].ErrorCode != "timeout" || rows[1].CostBps != nil {
		t.Fatalf("read back %+v", rows)
	}
	if rows[0].InputAmount.Big().String() != "1000000000" {
		t.Fatalf("decimal read back %s", rows[0].InputAmount.Big())
	}

	// A second flush with nothing buffered writes nothing.
	if files, err := s.Flush(); files != 0 || err != nil {
		t.Fatalf("empty flush: %d %v", files, err)
	}
}

func TestFailedFlushKeepsRows(t *testing.T) {
	root := t.TempDir()
	blocker := filepath.Join(root, "quotes")
	if err := os.WriteFile(blocker, []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := NewPartitioned(blocker, func(q model.Quote) string { return "dt=x" }, 1000, quietLog())
	s.Add(model.Quote{QuoteID: "a", TS: time.Now()})
	if _, err := s.Flush(); err == nil {
		t.Fatal("expected an error writing under a file")
	}
	if s.Buffered() != 1 {
		t.Fatalf("rows lost after failed flush: %d", s.Buffered())
	}
}
