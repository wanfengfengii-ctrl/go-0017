package money

import (
	"fmt"
	"math/big"
)

// Tolerance describes how much two amounts may differ and still be considered a
// match. Both components are optional (nil = disabled); a record satisfies the
// tolerance when every enabled component is satisfied. The boundary is closed:
// a difference exactly equal to the tolerance is a match, a difference one
// minor unit beyond it is a mismatch.
//
// No float64 is used anywhere in the comparison.
type Tolerance struct {
	// Abs is an absolute tolerance in minor currency units. nil disables it.
	Abs *int64
	// PctBps is a percentage tolerance in basis points (1 bp = 0.01%). nil
	// disables it. The comparison is |a-b| * 10000 <= PctBps * base, where base
	// is the larger of the two absolute amounts. Using basis points keeps the
	// percentage rational and integer-only.
	PctBps *int64
}

// NewAbsTolerance returns a tolerance with only the absolute component set.
func NewAbsTolerance(minor int64) Tolerance { v := minor; return Tolerance{Abs: &v} }

// NewPctTolerance returns a tolerance with only the percentage component set.
// bps is basis points (50 == 0.5%).
func NewPctTolerance(bps int64) Tolerance { v := bps; return Tolerance{PctBps: &v} }

// Enabled reports whether any tolerance component is configured.
func (t Tolerance) Enabled() bool { return t.Abs != nil || t.PctBps != nil }

// Within reports whether the absolute difference `diff` (minor units) between
// two amounts whose larger magnitude is `base` (minor units) is within the
// tolerance. base must be non-negative.
func (t Tolerance) Within(diff, base int64) bool {
	if diff < 0 {
		diff = -diff
	}
	if base < 0 {
		base = -base
	}
	if t.Abs != nil {
		if diff > *t.Abs {
			return false
		}
	}
	if t.PctBps != nil {
		// |diff| * 10000 <= pctBps * base, evaluated with big.Int to avoid
		// overflow on large payment amounts.
		lhs := new(big.Int).Mul(big.NewInt(diff), big.NewInt(10000))
		rhs := new(big.Int).Mul(big.NewInt(*t.PctBps), big.NewInt(base))
		if lhs.Cmp(rhs) > 0 {
			return false
		}
	}
	return true
}

// CompareTolerance returns -1/0/+1 describing whether diff is below, at, or
// above the effective tolerance. It is used by tests to assert boundary
// behaviour precisely.
func CompareTolerance(t Tolerance, diff, base int64) int {
	if diff < 0 {
		diff = -diff
	}
	if base < 0 {
		base = -base
	}
	// The "effective" boundary is the smaller of the two enabled boundaries
	// (since Within requires both). Report position relative to that boundary.
	bound := int64(-1)
	if t.Abs != nil {
		bound = *t.Abs
	}
	if t.PctBps != nil {
		// pct boundary = ceil(pctBps * base / 10000) — the largest diff that
		// still satisfies the percentage test.
		num := new(big.Int).Mul(big.NewInt(*t.PctBps), big.NewInt(base))
		quo := new(big.Int)
		rem := new(big.Int)
		quo.QuoRem(num, big.NewInt(10000), rem)
		if !quo.IsInt64() {
			// arbitrarily large; treat as unbounded above any practical diff.
			pct := int64(1<<62 - 1)
			if bound < 0 || pct < bound {
				bound = pct
			}
		} else {
			pct := quo.Int64()
			if rem.Sign() > 0 {
				// non-integral boundary: the inclusive max integer diff is quo.
				// (diff == quo still satisfies; diff == quo+1 does not.)
			}
			if bound < 0 || pct < bound {
				bound = pct
			}
		}
	}
	if bound < 0 {
		// No tolerance configured: only an exact (zero) difference matches.
		switch {
		case diff == 0:
			return 0
		default:
			return 1
		}
	}
	switch {
	case diff < bound:
		return -1
	case diff == bound:
		return 0
	default:
		return 1
	}
}

// String renders a tolerance for diagnostics.
func (t Tolerance) String() string {
	parts := make([]string, 0, 2)
	if t.Abs != nil {
		parts = append(parts, fmt.Sprintf("abs=%d", *t.Abs))
	}
	if t.PctBps != nil {
		parts = append(parts, fmt.Sprintf("pct=%dbps", *t.PctBps))
	}
	if len(parts) == 0 {
		return "tolerance(none)"
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "," + p
	}
	return "tolerance(" + out + ")"
}
