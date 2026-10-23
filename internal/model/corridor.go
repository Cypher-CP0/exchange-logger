// Package model holds the chains, tokens, corridors and the normalized
// quote record that every source is reduced to.
package model

import (
	"fmt"
	"sort"
	"strings"
)

// Chain is a blockchain the logger quotes on.
type Chain struct {
	Key    string // short key used in corridor ids: sol, base, arb, eth
	Name   string
	LifiID int64  // LI.FI numeric chain id
	VM     string // "svm" or "evm"
}

var (
	Solana   = Chain{Key: "sol", Name: "solana", LifiID: 1151111081099710, VM: "svm"}
	Base     = Chain{Key: "base", Name: "base", LifiID: 8453, VM: "evm"}
	Arbitrum = Chain{Key: "arb", Name: "arbitrum", LifiID: 42161, VM: "evm"}
	Ethereum = Chain{Key: "eth", Name: "ethereum", LifiID: 1, VM: "evm"}
)

// Chains lists every chain the logger knows.
func Chains() []Chain { return []Chain{Solana, Base, Arbitrum, Ethereum} }

// ChainByName resolves a key or common name ("sol", "solana", "arb",
// "arbitrum", "eth", "ethereum", "mainnet", "base") case-insensitively.
func ChainByName(name string) (Chain, bool) {
	n := strings.ToLower(strings.TrimSpace(name))
	if n == "mainnet" {
		n = "eth"
	}
	for _, c := range Chains() {
		if n == c.Key || n == c.Name {
			return c, true
		}
	}
	return Chain{}, false
}

// CorridorID builds the id for a token pair from chain keys and symbols.
func CorridorID(fromChain, fromToken, toChain, toToken string) string {
	return fmt.Sprintf("%s-%s__%s-%s", fromChain, strings.ToLower(fromToken), toChain, strings.ToLower(toToken))
}

// Token is a specific asset on a specific chain.
type Token struct {
	Symbol      string // USDC, SOL, ETH
	Chain       Chain
	Address     string // canonical mint or contract; wrapped SOL mint for SOL, zero address for native ETH
	Decimals    int
	PriceSymbol string // key into the round's price snapshot
	Native      bool   // the chain's gas token (SOL on Solana, ETH on EVM chains)
}

// Corridor is one directed route: send From, receive To.
type Corridor struct {
	ID   string // e.g. base-usdc__sol-usdc
	From Token
	To   Token
}

// NewCorridor builds a corridor and derives its id from the two tokens.
func NewCorridor(from, to Token) Corridor {
	return Corridor{
		ID:   CorridorID(from.Chain.Key, from.Symbol, to.Chain.Key, to.Symbol),
		From: from,
		To:   to,
	}
}

// SameChain reports whether the corridor is a swap on one chain rather than a bridge.
func (c Corridor) SameChain() bool { return c.From.Chain.Key == c.To.Chain.Key }

// Tokens tracked by the MVP.
var (
	SolUSDC = Token{Symbol: "USDC", Chain: Solana, Address: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", Decimals: 6, PriceSymbol: "USDC"}
	SolSOL  = Token{Symbol: "SOL", Chain: Solana, Address: "So11111111111111111111111111111111111111112", Decimals: 9, PriceSymbol: "SOL", Native: true}

	BaseUSDC = Token{Symbol: "USDC", Chain: Base, Address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", Decimals: 6, PriceSymbol: "USDC"}
	BaseETH  = Token{Symbol: "ETH", Chain: Base, Address: "0x0000000000000000000000000000000000000000", Decimals: 18, PriceSymbol: "ETH", Native: true}

	ArbUSDC = Token{Symbol: "USDC", Chain: Arbitrum, Address: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", Decimals: 6, PriceSymbol: "USDC"}

	EthUSDC = Token{Symbol: "USDC", Chain: Ethereum, Address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", Decimals: 6, PriceSymbol: "USDC"}
)

// DefaultCorridors is the MVP set from the design doc: everything settles to
// USDC on Solana, plus ETH -> USDC on Base.
func DefaultCorridors() []Corridor {
	return []Corridor{
		NewCorridor(SolSOL, SolUSDC),   // sol-sol__sol-usdc
		NewCorridor(BaseUSDC, SolUSDC), // base-usdc__sol-usdc
		NewCorridor(ArbUSDC, SolUSDC),  // arb-usdc__sol-usdc
		NewCorridor(EthUSDC, SolUSDC),  // eth-usdc__sol-usdc
		NewCorridor(BaseETH, SolUSDC),  // base-eth__sol-usdc
		NewCorridor(BaseETH, BaseUSDC), // base-eth__base-usdc
	}
}

// SelectCorridors returns the default corridors whose ids are in ids.
// An empty list selects all of them.
func SelectCorridors(ids []string) ([]Corridor, error) {
	all := DefaultCorridors()
	if len(ids) == 0 {
		return all, nil
	}
	byID := make(map[string]Corridor, len(all))
	for _, c := range all {
		byID[c.ID] = c
	}
	out := make([]Corridor, 0, len(ids))
	for _, id := range ids {
		c, ok := byID[id]
		if !ok {
			known := make([]string, 0, len(all))
			for _, c := range all {
				known = append(known, c.ID)
			}
			return nil, fmt.Errorf("unknown corridor %q (known: %s)", id, strings.Join(known, ", "))
		}
		out = append(out, c)
	}
	return out, nil
}

// PriceSymbols lists every symbol a round needs priced: the corridor tokens
// plus the gas tokens used to convert gas costs to USD.
func PriceSymbols(corridors []Corridor) []string {
	seen := map[string]bool{"SOL": true, "ETH": true, "USDC": true}
	for _, c := range corridors {
		seen[c.From.PriceSymbol] = true
		seen[c.To.PriceSymbol] = true
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
