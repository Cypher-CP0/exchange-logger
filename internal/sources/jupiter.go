package sources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Cypher-CP0/exchange-logger/internal/model"
	"github.com/Cypher-CP0/exchange-logger/internal/prices"
)

// Jupiter quotes same-chain Solana swaps through the Swap API v2 /order
// endpoint (GET https://api.jup.ag/swap/v2/order). Called without a taker,
// /order returns a quote and no transaction, which is all the logger needs.
//
// Fee accounting:
//   - outAmount is already net of Jupiter's platform/gasless fees (feeBps),
//     so ExtraFeeUSD is 0 and ProtocolFeeUSD is informational.
//   - Gas is signatureFeeLamports + prioritizationFeeLamports when the
//     response reports them. rentFeeLamports is left out: it pays for
//     account creation and is mostly refundable.
type Jupiter struct {
	OrderURL    string // e.g. https://api.jup.ag/swap/v2/order
	APIKey      string // sent as x-api-key
	SlippageBps int
	Client      *http.Client
}

func (j *Jupiter) Name() string { return "jupiter" }

func (j *Jupiter) Supports(c model.Corridor) bool {
	return c.SameChain() && c.From.Chain.Key == model.Solana.Key
}

func (j *Jupiter) Quote(ctx context.Context, req Request) (Raw, error) {
	q := url.Values{}
	q.Set("inputMint", req.Corridor.From.Address)
	q.Set("outputMint", req.Corridor.To.Address)
	q.Set("amount", req.InputAmount.String())
	q.Set("slippageBps", strconv.Itoa(j.SlippageBps))
	q.Set("swapMode", "ExactIn")
	return getJSON(ctx, j.Client, j.OrderURL+"?"+q.Encode(), map[string]string{"x-api-key": j.APIKey}, jupiterClassify)
}

// jupiterClassify maps 4xx bodies that say there is no route to no_route.
// Jupiter returns {"error": "..."} on 400; we match on the text because the
// v2 API documents no stable code for the no-route case without a taker.
func jupiterClassify(status int, body []byte) *Error {
	if status < 400 || status >= 500 || status == http.StatusTooManyRequests || status == http.StatusUnauthorized || status == http.StatusForbidden {
		return nil
	}
	var e struct {
		Error        string `json:"error"`
		ErrorMessage string `json:"errorMessage"`
	}
	_ = json.Unmarshal(body, &e)
	msg := e.Error
	if msg == "" {
		msg = e.ErrorMessage
	}
	if isNoRouteText(msg) {
		return &Error{Code: CodeNoRoute, Detail: truncate(msg, 300)}
	}
	return nil
}

func isNoRouteText(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "no route") || strings.Contains(m, "route not found") ||
		strings.Contains(m, "could not find any route") || strings.Contains(m, "no routes")
}

type jupiterOrder struct {
	InAmount                  string   `json:"inAmount"`
	OutAmount                 string   `json:"outAmount"`
	OtherAmountThreshold      string   `json:"otherAmountThreshold"`
	Router                    string   `json:"router"`
	SwapType                  string   `json:"swapType"` // deprecated alias of router
	FeeBps                    *float64 `json:"feeBps"`
	SignatureFeeLamports      *float64 `json:"signatureFeeLamports"`
	PrioritizationFeeLamports *float64 `json:"prioritizationFeeLamports"`
	RoutePlan                 []struct {
		SwapInfo struct {
			Label string `json:"label"`
		} `json:"swapInfo"`
	} `json:"routePlan"`
	Error        string `json:"error"`
	ErrorMessage string `json:"errorMessage"`
}

func (j *Jupiter) Normalize(req Request, raw Raw, snap prices.Snapshot) (Result, error) {
	var o jupiterOrder
	if err := json.Unmarshal(raw, &o); err != nil {
		return Result{}, &Error{Code: CodeBadResponse, Detail: "decode: " + err.Error()}
	}
	if o.Error != "" || o.ErrorMessage != "" {
		msg := o.Error
		if msg == "" {
			msg = o.ErrorMessage
		}
		// Without a taker Jupiter should not send transaction-build errors,
		// but a body-level error still means no usable quote.
		code := CodeBadResponse
		if isNoRouteText(msg) {
			code = CodeNoRoute
		}
		return Result{}, &Error{Code: code, Detail: truncate(msg, 300)}
	}
	out, ok := parseUnits(o.OutAmount)
	if !ok {
		return Result{}, &Error{Code: CodeBadResponse, Detail: "missing outAmount"}
	}
	if out.Sign() == 0 {
		return Result{}, &Error{Code: CodeNoRoute, Detail: "outAmount is 0"}
	}
	res := Result{ExpectedOutput: out}
	if min, ok := parseUnits(o.OtherAmountThreshold); ok {
		res.MinOutput = min
	}

	labels := make([]string, 0, len(o.RoutePlan))
	for _, step := range o.RoutePlan {
		labels = append(labels, step.SwapInfo.Label)
	}
	router := o.Router
	if router == "" {
		router = o.SwapType
	}
	res.RouteLabel = distinctJoin(labels, "+")
	if router != "" {
		if res.RouteLabel != "" {
			res.RouteLabel = router + ":" + res.RouteLabel
		} else {
			res.RouteLabel = router
		}
	}

	if o.SignatureFeeLamports != nil || o.PrioritizationFeeLamports != nil {
		if solUSD, ok := snap.Price("SOL"); ok {
			lamports := derefF(o.SignatureFeeLamports) + derefF(o.PrioritizationFeeLamports)
			res.GasUSDSrc = model.Ptr(lamports / 1e9 * solUSD)
		}
	}
	if o.FeeBps != nil {
		res.ProtocolFeeUSD = model.Ptr(req.InputUSD * *o.FeeBps / 10000)
	}
	return res, nil
}

func derefF(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
