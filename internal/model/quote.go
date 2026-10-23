package model

import (
	"errors"
	"math"
	"math/big"
	"time"
)

// Quote is the normalized record every source is reduced to. One row is
// written per quote attempt, including failed ones: availability rankings
// depend on the failures.
//
// Optional columns use parquet-go's zero-means-null rule: an empty string or
// a zero Decimal is written as NULL, and pointer fields are NULL when nil.
type Quote struct {
	QuoteID     string    `parquet:"quote_id"`
	PollRound   int64     `parquet:"poll_round"` // unix ms of the round start; shared by every quote in the round
	TS          time.Time `parquet:"ts,timestamp(millisecond)"`
	Source      string    `parquet:"source,dict"`
	RouteLabel  string    `parquet:"route_label,optional,dict"` // the source's internal route, e.g. the bridge LI.FI picked
	Corridor    string    `parquet:"corridor,dict"`
	SizeBucket  int32     `parquet:"size_bucket"` // 100, 1000 or 10000 (USD)
	InputToken  string    `parquet:"input_token,dict"`
	OutputToken string    `parquet:"output_token,dict"`

	InputAmount    Decimal `parquet:"input_amount,decimal(0:38)"`             // raw token units
	ExpectedOutput Decimal `parquet:"expected_output,decimal(0:38),optional"` // raw token units
	MinOutput      Decimal `parquet:"min_output,decimal(0:38),optional"`      // raw token units, after slippage

	InputUSD       float64  `parquet:"input_usd"`
	OutputUSD      *float64 `parquet:"output_usd,optional"`
	GasUSDSrc      *float64 `parquet:"gas_usd_src,optional"`
	GasUSDDst      *float64 `parquet:"gas_usd_dst,optional"`
	ProtocolFeeUSD *float64 `parquet:"protocol_fee_usd,optional"` // all bridge/relayer/aggregator fees, informational
	ExtraFeeUSD    *float64 `parquet:"extra_fee_usd,optional"`    // the part of those fees NOT already deducted from expected_output
	EstSeconds     *int32   `parquet:"est_seconds,optional"`
	CostBps        *float64 `parquet:"cost_bps,optional"`

	LatencyMs   int32  `parquet:"latency_ms"`
	QuoteOK     bool   `parquet:"quote_ok"`
	ErrorCode   string `parquet:"error_code,optional,dict"`
	ErrorDetail string `parquet:"error_detail,optional"`
}

// PriceRow records one reference price used in a round, so every cost_bps
// can be recomputed later. Venue is "binance", "okx", or "ref" for the
// value the round actually used.
type PriceRow struct {
	PollRound int64     `parquet:"poll_round"`
	TS        time.Time `parquet:"ts,timestamp(millisecond)"`
	Symbol    string    `parquet:"symbol,dict"`
	Venue     string    `parquet:"venue,dict"`
	PriceUSD  float64   `parquet:"price_usd"`
}

// CostBps is the effective cost from the design doc: everything the user
// loses between what they send and what arrives, relative to the reference
// price of the input.
//
//	cost_bps = (input_usd - (output_usd - gas_src - gas_dst - fees_not_deducted)) / input_usd * 10000
func CostBps(inputUSD, outputUSD float64, gasSrc, gasDst *float64, extraFee float64) float64 {
	net := outputUSD - deref(gasSrc) - deref(gasDst) - extraFee
	return (inputUSD - net) / inputUSD * 10000
}

func deref(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// Ptr returns a pointer to v; handy for the optional columns.
func Ptr[T any](v T) *T { return &v }

// Decimal is a DECIMAL(38,0) stored as 16 big-endian bytes, which is how
// Parquet encodes decimals of that precision. Only non-negative values are
// used here (token amounts).
type Decimal [16]byte

var errDecimalRange = errors.New("value does not fit DECIMAL(38,0)")

var maxDecimal = new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(38), nil), big.NewInt(1))

// DecimalFromBig converts a non-negative integer to a Decimal.
func DecimalFromBig(v *big.Int) (Decimal, error) {
	var d Decimal
	if v == nil {
		return d, nil
	}
	if v.Sign() < 0 || v.Cmp(maxDecimal) > 0 {
		return d, errDecimalRange
	}
	v.FillBytes(d[:])
	return d, nil
}

// Big converts the Decimal back to a big.Int.
func (d Decimal) Big() *big.Int { return new(big.Int).SetBytes(d[:]) }

// ToUnits converts a human amount (e.g. 1000.5 USDC) to raw token units,
// rounding down.
func ToUnits(amount float64, decimals int) *big.Int {
	f := new(big.Float).SetPrec(256).SetFloat64(amount)
	f.Mul(f, new(big.Float).SetPrec(256).SetInt(pow10(decimals)))
	out, _ := f.Int(nil)
	return out
}

// FromUnits converts raw token units to a human amount.
func FromUnits(units *big.Int, decimals int) float64 {
	if units == nil {
		return 0
	}
	f := new(big.Float).SetPrec(256).SetInt(units)
	f.Quo(f, new(big.Float).SetPrec(256).SetInt(pow10(decimals)))
	out, _ := f.Float64()
	return out
}

func pow10(n int) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }

// Round2 rounds to 2 decimal places; used to keep cost_bps readable.
func Round2(v float64) float64 { return math.Round(v*100) / 100 }
