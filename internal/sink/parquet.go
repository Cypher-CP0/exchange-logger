// Package sink buffers rows in memory and flushes them to immutable,
// Hive-partitioned Parquet files that DuckDB can query directly.
//
// Files are written under a hidden temporary name and renamed when complete,
// so a reader globbing *.parquet never sees a half-written file.
package sink

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Partitioned buffers rows per partition directory and flushes them to one
// Parquet file per partition.
type Partitioned[T any] struct {
	root      string         // e.g. data/quotes
	partition func(T) string // relative dir, e.g. dt=2026-10-22/corridor=base-usdc__sol-usdc
	maxRows   int            // flush early when this many rows are buffered
	log       *slog.Logger
	now       func() time.Time

	mu    sync.Mutex
	buf   map[string][]T
	count int

	flushMu sync.Mutex // one flush at a time
	seq     int
	kick    chan struct{}
}

// NewPartitioned creates a sink rooted at root.
func NewPartitioned[T any](root string, partition func(T) string, maxRows int, log *slog.Logger) *Partitioned[T] {
	return &Partitioned[T]{
		root:      root,
		partition: partition,
		maxRows:   maxRows,
		log:       log,
		now:       time.Now,
		buf:       map[string][]T{},
		kick:      make(chan struct{}, 1),
	}
}

// Add buffers rows. Crossing maxRows triggers an early flush.
func (p *Partitioned[T]) Add(rows ...T) {
	if len(rows) == 0 {
		return
	}
	p.mu.Lock()
	// If flushes keep failing (disk full, permissions) don't grow forever:
	// past 10x maxRows new rows are dropped, loudly.
	if p.maxRows > 0 && p.count >= 10*p.maxRows {
		p.mu.Unlock()
		p.log.Error("buffer full, dropping rows", "event", "rows_dropped", "root", p.root, "rows", len(rows))
		return
	}
	for _, r := range rows {
		k := p.partition(r)
		p.buf[k] = append(p.buf[k], r)
	}
	p.count += len(rows)
	full := p.maxRows > 0 && p.count >= p.maxRows
	p.mu.Unlock()
	if full {
		select {
		case p.kick <- struct{}{}:
		default:
		}
	}
}

// Buffered returns the number of rows waiting to be flushed.
func (p *Partitioned[T]) Buffered() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}

// Run flushes every interval, or early when the buffer fills, until ctx is
// cancelled. It does not flush on exit; call Flush after the producer stops.
func (p *Partitioned[T]) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-p.kick:
		}
		if _, err := p.Flush(); err != nil {
			p.log.Error("flush failed; rows kept for the next attempt", "event", "flush_failed", "root", p.root, "error", err.Error())
		}
	}
}

// Flush writes every buffered partition to a new file. Partitions that fail
// to write are put back in the buffer and retried on the next flush.
func (p *Partitioned[T]) Flush() (files int, err error) {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	p.mu.Lock()
	batch := p.buf
	p.buf = map[string][]T{}
	p.count = 0
	p.mu.Unlock()
	if len(batch) == 0 {
		return 0, nil
	}

	keys := make([]string, 0, len(batch))
	for k := range batch {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	stamp := p.now().UTC().Format("20060102T150405Z")
	var firstErr error
	rows := 0
	for _, k := range keys {
		path, werr := p.writeFile(k, stamp, batch[k])
		if werr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", k, werr)
			}
			p.mu.Lock()
			p.buf[k] = append(batch[k], p.buf[k]...)
			p.count += len(batch[k])
			p.mu.Unlock()
			continue
		}
		files++
		rows += len(batch[k])
		p.log.Debug("parquet file written", "event", "parquet_file_written", "path", path, "rows", len(batch[k]))
	}
	if files > 0 {
		p.log.Info("flushed", "event", "flush_complete", "root", p.root, "files", files, "rows", rows)
	}
	return files, firstErr
}

func (p *Partitioned[T]) writeFile(partition, stamp string, rows []T) (string, error) {
	dir := filepath.Join(p.root, filepath.FromSlash(partition))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	var final string
	for {
		p.seq++
		final = filepath.Join(dir, fmt.Sprintf("part-%s-%04d.parquet", stamp, p.seq))
		if _, err := os.Stat(final); os.IsNotExist(err) {
			break
		}
	}
	tmp := filepath.Join(dir, "."+filepath.Base(final)+".tmp")

	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	cleanup := func(e error) (string, error) {
		f.Close()
		os.Remove(tmp)
		return "", e
	}
	w := parquet.NewGenericWriter[T](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		return cleanup(err)
	}
	if err := w.Close(); err != nil {
		return cleanup(err)
	}
	if err := f.Sync(); err != nil {
		return cleanup(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return final, nil
}

// DatePartition formats a UTC day as a Hive partition segment.
func DatePartition(t time.Time) string { return "dt=" + t.UTC().Format("2006-01-02") }
