//go:build n1ql

package test

import (
	"os"
	"testing"
	"time"
)

// TestMain pins the process timezone to UTC for the whole test package. Several
// N1QL date functions (MILLIS_TO_STR, MILLIS_TO_LOCAL, the calendar DATE_*_MILLIS
// family, ...) format epoch millis through the local zone, so their results --
// and the corpus's expected values -- are only reproducible under a fixed zone.
// Without this, a handful of date cases pass in UTC/US/EU but fail under extreme
// zones (e.g. Pacific/Auckland at +13). Pinning UTC makes the suite deterministic
// regardless of the host's TZ.
func TestMain(m *testing.M) {
	time.Local = time.UTC
	os.Exit(m.Run())
}
