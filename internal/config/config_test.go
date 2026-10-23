package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultsAndLiFiBudget(t *testing.T) {
	t.Setenv("LIFI_API_KEY", "")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.PollInterval != 30*time.Second || c.FlushInterval != 5*time.Minute || c.FlushMaxRows != 50000 {
		t.Fatalf("timing defaults: %+v", c)
	}
	if len(c.Corridors) != 6 || len(c.SizeBuckets) != 3 || !c.Sources["jupiter"] || !c.Sources["lifi"] {
		t.Fatalf("scope defaults: %d corridors, sizes %v, sources %v", len(c.Corridors), c.SizeBuckets, c.Sources)
	}
	if c.LiFiRPS > 0.011 {
		t.Fatalf("without a key LI.FI must stay under 75 quotes / 2h, got %v rps", c.LiFiRPS)
	}

	t.Setenv("LIFI_API_KEY", "k")
	c, _ = Load()
	if c.LiFiRPS != 1.5 {
		t.Fatalf("keyed LI.FI rps %v", c.LiFiRPS)
	}
}

func TestInvalidValuesAreReported(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "soon")
	t.Setenv("SOURCES", "jupiter,uniswap")
	t.Setenv("CORRIDORS", "mars-usdc__sol-usdc")
	if _, err := Load(); err == nil {
		t.Fatal("expected errors")
	}
}

func TestDotEnvDoesNotOverrideRealEnv(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, ".env")
	os.WriteFile(p, []byte("# comment\nEXLOG_A=from_file   # trailing note\nexport EXLOG_B=\"quoted # kept\"\nEXLOG_C=from_file\n"), 0o644)
	t.Setenv("EXLOG_C", "from_env")
	os.Unsetenv("EXLOG_A")
	os.Unsetenv("EXLOG_B")
	if err := LoadDotEnv(p); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("EXLOG_A") != "from_file" || os.Getenv("EXLOG_B") != "quoted # kept" || os.Getenv("EXLOG_C") != "from_env" {
		t.Fatalf("got A=%q B=%q C=%q", os.Getenv("EXLOG_A"), os.Getenv("EXLOG_B"), os.Getenv("EXLOG_C"))
	}
	os.Unsetenv("EXLOG_A")
	os.Unsetenv("EXLOG_B")
	if err := LoadDotEnv(filepath.Join(dir, "missing.env")); err != nil {
		t.Fatal("a missing .env is fine")
	}
}
