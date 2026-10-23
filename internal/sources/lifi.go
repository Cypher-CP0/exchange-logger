package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
)

// LiFi quotes every corridor (bridges and same-chain swaps) through
// GET https://li.quest/v1/quote, which returns LI.FI's best route.
//
// Fee accounting:
//   - estimate.toAmount is what arrives; fees with included=true are already
//     deducted from it. Fees with included=false are paid on top (usually in
//     the source chain's gas token) and go into ExtraFeeUSD.
//   - ProtocolFeeUSD is the sum of all feeCosts, informational.
//   - estimate.gasCosts are source-chain transactions and go into GasUSDSrc.
//     LI.FI does not report destination gas separately (relayers bake it
//     into fees), so GasUSDDst stays NULL.
//   - Gas and fees are re-valued with the round's own price snapshot when the
//     token is one we price (ETH, SOL, USDC), so every source in a round is
//     costed against the same prices. Other tokens fall back to LI.FI's
//     amountUSD.
//
// Rate limits (docs.li.fi): without an API key /v1/quote allows 75 requests
// per 2 hours. With a key (x-lifi-api-key) the default is 100 per minute.
type LiFi struct {
	BaseURL       string // https://li.quest
	APIKey        string
	Integrator    string // optional
	SlippageBps   int
	Order         string // CHEAPEST or FASTEST
	EVMAddress    string // quote-only sender/recipient on EVM chains; never holds funds
	SolanaAddress string // quote-only sender/recipient on Solana
	Client        *http.Client
}

func (l *LiFi) Name() string { return "lifi" }

func (l *LiFi) Supports(c model.Corridor) bool {
	return c.From.Chain.LifiID != 0 && c.To.Chain.LifiID != 0
}

// lifiToken is the address LI.FI expects; native gas tokens use LI.FI's
// placeholder addresses rather than the wrapped mint we store.
func lifiToken(t model.Token) string {
	if t.Native {
		if t.Chain.VM == "svm" {
			return "11111111111111111111111111111111"
		}
		return "0x0000000000000000000000000000000000000000"
	}
	return t.Address
}

func (l *LiFi) addressFor(c model.Chain) string {
	if c.VM == "svm" {
		return l.SolanaAddress
	}
	return l.EVMAddress
}

func (l *LiFi) Quote(ctx context.Context, req Request) (Raw, error) {
	c := req.Corridor
	q := url.Values{}
	q.Set("fromChain", strconv.FormatInt(c.From.Chain.LifiID, 10))
	q.Set("toChain", strconv.FormatInt(c.To.Chain.LifiID, 10))
	q.Set("fromToken", lifiToken(c.From))
	q.Set("toToken", lifiToken(c.To))
	q.Set("fromAmount", req.InputAmount.String())
	q.Set("fromAddress", l.addressFor(c.From.Chain))
	q.Set("toAddress", l.addressFor(c.To.Chain))
	q.Set("slippage", strconv.FormatFloat(float64(l.SlippageBps)/10000, 'f', -1, 64))
	if l.Order != "" {
		q.Set("order", l.Order)
	}
	if l.Integrator != "" {
		q.Set("integrator", l.Integrator)
	}
	u := strings.TrimRight(l.BaseURL, "/") + "/v1/quote?" + q.Encode()
	return getJSON(ctx, l.Client, u, map[string]string{"x-lifi-api-key": l.APIKey}, lifiClassify)
}

// LI.FI error bodies look like {"message": "...", "code": 1002}.
// 1002 is "no available quotes"; 1005 is its rate-limit error.
func lifiClassify(status int, body []byte) *Error {
	var e struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	}
	_ = json.Unmarshal(body, &e)
	switch {
	case e.Code == 1002 || (status == http.StatusNotFound && strings.Contains(strings.ToLower(e.Message), "no available quotes")):
		return &Error{Code: CodeNoRoute, Detail: truncate(e.Message, 300)}
	case e.Code == 1005:
		return &Error{Code: CodeRateLimited, Detail: truncate(e.Message, 300)}
	}
	return nil
}

type lifiTokenInfo struct {
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

type lifiCost struct {
	Name      string        `json:"name"`
	Type      string        `json:"type"`
	Amount    string        `json:"amount"`
	AmountUSD flexFloat     `json:"amountUSD"`
	Included  *bool         `json:"included"`
	Token     lifiTokenInfo `json:"token"`
}

type lifiQuote struct {
	Tool     string `json:"tool"`
	Estimate struct {
		ToAmount          string     `json:"toAmount"`
		ToAmountMin       string     `json:"toAmountMin"`
		ExecutionDuration flexFloat  `json:"executionDuration"`
		GasCosts          []lifiCost `json:"gasCosts"`
		FeeCosts          []lifiCost `json:"feeCosts"`
	} `json:"estimate"`
	IncludedSteps []struct {
		Tool string `json:"tool"`
		Type string `json:"type"`
	} `json:"includedSteps"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

func (l *LiFi) Normalize(req Request, raw Raw, snap prices.Snapshot) (Result, error) {
	var q lifiQuote
	if err := json.Unmarshal(raw, &q); err != nil {
		return Result{}, &Error{Code: CodeBadResponse, Detail: "decode: " + err.Error()}
	}
	if q.Code != 0 && q.Estimate.ToAmount == "" {
		if e := lifiClassify(http.StatusOK, raw); e != nil {
			return Result{}, e
		}
		return Result{}, &Error{Code: CodeBadResponse, Detail: fmt.Sprintf("code %d: %s", q.Code, truncate(q.Message, 280))}
	}
	out, ok := parseUnits(q.Estimate.ToAmount)
	if !ok {
		return Result{}, &Error{Code: CodeBadResponse, Detail: "missing estimate.toAmount"}
	}
	if out.Sign() == 0 {
		return Result{}, &Error{Code: CodeNoRoute, Detail: "toAmount is 0"}
	}
	res := Result{ExpectedOutput: out}
	if min, ok := parseUnits(q.Estimate.ToAmountMin); ok {
		res.MinOutput = min
	}

	steps := make([]string, 0, len(q.IncludedSteps))
	for _, s := range q.IncludedSteps {
		if s.Type == "protocol" || strings.EqualFold(s.Tool, "feeCollection") {
			continue // integrator fee collection, not a routing hop
		}
		steps = append(steps, s.Tool)
	}
	res.RouteLabel = distinctJoin(steps, "+")
	if res.RouteLabel == "" {
		res.RouteLabel = q.Tool
	}

	if len(q.Estimate.GasCosts) > 0 {
		var gas float64
		for _, g := range q.Estimate.GasCosts {
			gas += valueUSD(g, snap)
		}
		res.GasUSDSrc = model.Ptr(gas)
	}
	if len(q.Estimate.FeeCosts) > 0 {
		var all, extra float64
		for _, f := range q.Estimate.FeeCosts {
			v := valueUSD(f, snap)
			all += v
			if f.Included != nil && !*f.Included {
				extra += v
			}
		}
		res.ProtocolFeeUSD = model.Ptr(all)
		res.ExtraFeeUSD = extra
	}
	if q.Estimate.ExecutionDuration.Valid {
		res.EstSeconds = model.Ptr(int32(math.Round(q.Estimate.ExecutionDuration.V)))
	}
	return res, nil
}

// valueUSD prices a cost line with the round's snapshot when we price that
// token, falling back to LI.FI's own amountUSD.
func valueUSD(c lifiCost, snap prices.Snapshot) float64 {
	if sym := snapshotSymbol(c.Token.Symbol); sym != "" {
		if p, ok := snap.Price(sym); ok {
			if units, ok := parseUnits(c.Amount); ok && c.Token.Decimals > 0 {
				return model.FromUnits(units, c.Token.Decimals) * p
			}
		}
	}
	if c.AmountUSD.Valid {
		return c.AmountUSD.V
	}
	return 0
}

func snapshotSymbol(s string) string {
	switch strings.ToUpper(s) {
	case "ETH", "WETH":
		return "ETH"
	case "SOL", "WSOL":
		return "SOL"
	case "USDC":
		return "USDC"
	}
	return ""
}
