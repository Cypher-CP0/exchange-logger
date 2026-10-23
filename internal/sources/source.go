// Package sources holds one adapter per quote source. Each adapter turns a
// QuoteRequest into the source's API call and normalizes the response into
// the fields of model.Quote, so adding a source never touches the scheduler.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
)

// Request is one quote to fetch: a corridor, a size bucket, and the exact
// input amount derived from the round's price snapshot.
type Request struct {
	PollRound   int64
	Corridor    model.Corridor
	SizeBucket  int32
	InputAmount *big.Int // raw units of Corridor.From
	InputUSD    float64
}

// Raw is a source's response body, kept separate from normalization so
// normalization can be tested on fixtures.
type Raw []byte

// Result is what a source contributes to a quote row.
type Result struct {
	RouteLabel     string
	ExpectedOutput *big.Int // raw units of Corridor.To
	MinOutput      *big.Int
	GasUSDSrc      *float64 // nil when the source does not report it
	GasUSDDst      *float64
	ProtocolFeeUSD *float64 // all fees, informational
	ExtraFeeUSD    float64  // fees NOT already deducted from ExpectedOutput
	EstSeconds     *int32
}

// Source is the adapter interface from the design doc.
type Source interface {
	Name() string
	Supports(c model.Corridor) bool
	Quote(ctx context.Context, req Request) (Raw, error)
	Normalize(req Request, raw Raw, snap prices.Snapshot) (Result, error)
}

// Error codes written to the error_code column.
const (
	CodeTimeout      = "timeout"       // no response within the request timeout
	CodeNetwork      = "network"       // connection-level failure
	CodeRateLimited  = "rate_limited"  // HTTP 429 from the source
	CodeUnauthorized = "unauthorized"  // HTTP 401/403, usually a missing or bad API key
	CodeNoRoute      = "no_route"      // the source answered but has no route for this request
	CodeBadResponse  = "bad_response"  // undecodable or missing required fields
	CodeSourcePaused = "source_paused" // circuit open after repeated failed rounds; no request made
	// Other HTTP failures are recorded as http_<status>, e.g. http_500.
)

// Error is a classified quote failure.
type Error struct {
	Code       string
	Detail     string
	RetryAfter time.Duration // from a Retry-After header, when present
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// Classify turns any error into an *Error.
func Classify(err error) *Error {
	var se *Error
	if errors.As(err, &se) {
		return se
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: CodeTimeout}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return &Error{Code: CodeTimeout}
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: CodeTimeout, Detail: "canceled"}
	}
	return &Error{Code: CodeNetwork, Detail: err.Error()}
}

// statusClassifier lets a source map a non-2xx body to a more specific code
// (for example LI.FI's "no available quotes"). Returning nil falls back to
// the generic mapping.
type statusClassifier func(status int, body []byte) *Error

const maxBody = 2 << 20

// getJSON performs a GET and returns the body of a 2xx response, or a
// classified *Error.
func getJSON(ctx context.Context, client *http.Client, url string, headers map[string]string, classify statusClassifier) (Raw, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, &Error{Code: CodeBadResponse, Detail: err.Error()}
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "exchange-logger/0.1")
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, Classify(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, Classify(err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}
	if classify != nil {
		if e := classify(resp.StatusCode, body); e != nil {
			return nil, e
		}
	}
	detail := truncate(strings.TrimSpace(string(body)), 300)
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return nil, &Error{Code: CodeRateLimited, Detail: detail, RetryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, &Error{Code: CodeUnauthorized, Detail: detail}
	default:
		return nil, &Error{Code: fmt.Sprintf("http_%d", resp.StatusCode), Detail: detail}
	}
}

func retryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// parseUnits parses a raw integer amount string.
func parseUnits(s string) (*big.Int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	v, ok := new(big.Int).SetString(s, 10)
	if !ok || v.Sign() < 0 {
		return nil, false
	}
	return v, true
}

// flexFloat accepts a JSON number or a numeric string; LI.FI sends USD
// amounts as strings.
type flexFloat struct {
	V     float64
	Valid bool
}

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil // tolerate junk; the field is treated as absent
	}
	f.V, f.Valid = v, true
	return nil
}

var _ json.Unmarshaler = (*flexFloat)(nil)

// distinctJoin joins non-empty values in first-seen order.
func distinctJoin(values []string, sep string) string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return strings.Join(out, sep)
}
