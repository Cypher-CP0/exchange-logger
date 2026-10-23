package model

import (
	"math"
	"math/big"
	"testing"
)

func TestDecimalRoundTrip(t *testing.T) {
	for _, s := range []string{"0", "1", "998500000", "3000000000000000000", "99999999999999999999999999999999999999"} {
		v, _ := new(big.Int).SetString(s, 10)
		d, err := DecimalFromBig(v)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if got := d.Big().String(); got != s {
			t.Fatalf("round trip %s -> %s", s, got)
		}
	}
	tooBig, _ := new(big.Int).SetString("100000000000000000000000000000000000000", 10) // 10^38
	if _, err := DecimalFromBig(tooBig); err == nil {
		t.Fatal("10^38 must not fit DECIMAL(38,0)")
	}
	if _, err := DecimalFromBig(big.NewInt(-1)); err == nil {
		t.Fatal("negative amounts are rejected")
	}
}

func TestUnits(t *testing.T) {
	if got := ToUnits(1000, 6).String(); got != "1000000000" {
		t.Fatalf("1000 USDC = %s", got)
	}
	if got := ToUnits(100.0/2500.0, 18).String(); got != "40000000000000000" {
		t.Fatalf("$100 of ETH at 2500 = %s wei", got)
	}
	if got := FromUnits(big.NewInt(998500000), 6); math.Abs(got-998.5) > 1e-9 {
		t.Fatalf("FromUnits = %v", got)
	}
}

func TestCostBps(t *testing.T) {
	gas, dst := 0.5, 0.1
	// Sent $1000, $998 arrives, $0.50 + $0.10 gas, $0.25 fee paid on top:
	// loss = 1000 - (998 - 0.6 - 0.25) = 2.85 -> 28.5 bps
	got := CostBps(1000, 998, &gas, &dst, 0.25)
	if math.Abs(got-28.5) > 1e-9 {
		t.Fatalf("cost = %v", got)
	}
	if got := CostBps(1000, 1000, nil, nil, 0); got != 0 {
		t.Fatalf("free route = %v", got)
	}
}

func TestCorridors(t *testing.T) {
	ids := map[string]bool{}
	for _, c := range DefaultCorridors() {
		ids[c.ID] = true
	}
	for _, want := range []string{"sol-sol__sol-usdc", "base-usdc__sol-usdc", "arb-usdc__sol-usdc", "eth-usdc__sol-usdc", "base-eth__sol-usdc", "base-eth__base-usdc"} {
		if !ids[want] {
			t.Errorf("missing corridor %s", want)
		}
	}
	if _, err := SelectCorridors([]string{"nope"}); err == nil {
		t.Error("unknown corridor must error")
	}
	sel, err := SelectCorridors([]string{"base-usdc__sol-usdc"})
	if err != nil || len(sel) != 1 || sel[0].SameChain() {
		t.Fatalf("select: %v %v", sel, err)
	}
}
