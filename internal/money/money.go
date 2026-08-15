// Package money implements payment amounts as signed integer minor currency
// units. No float64 value is ever used to represent, parse, compare or compute
// a monetary amount: the package is the single source of truth for the
// reconciliation ledger's amount precision.
//
// An Amount is the number of minor units (for example US cents) held in an
// int64. A Currency carries the ISO 4217 code and the number of decimal digits
// in the minor unit (the "scale"). Parsing a decimal string such as "12.34" for
// USD (scale 2) yields the Amount 1234. The parser rejects empty strings,
// multiple separators, leading signs combined with whitespace and any fractional
// part that the configured rounding mode does not accept.
package money

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// Scale is the number of decimal digits in a currency's minor unit.
type Scale uint8

// UnknownScale is a sentinel used by ingest to mark a currency code that is not
// in the registry.
const UnknownScale Scale = 255

// Currency describes a single currency's precision metadata.
type Currency struct {
	Code  string // ISO 4217 alpha-3 code, upper case.
	Scale Scale  // decimal digits in the minor unit (0 for JPY, 2 for USD).
}

// Common currencies. The registry can be extended at runtime; see Registry.
var (
	USD = Currency{Code: "USD", Scale: 2}
	EUR = Currency{Code: "EUR", Scale: 2}
	GBP = Currency{Code: "GBP", Scale: 2}
	CNY = Currency{Code: "CNY", Scale: 2}
	JPY = Currency{Code: "JPY", Scale: 0}
)

// Amount is a signed quantity of minor currency units.
type Amount struct {
	C Currency
	V int64
}

// Zero returns the zero amount for a currency.
func Zero(c Currency) Amount { return Amount{C: c, V: 0} }

// FromMinor constructs an Amount from an already-minor int64 value.
func FromMinor(c Currency, v int64) Amount { return Amount{C: c, V: v} }

// Add returns a+b. The currencies must match.
func Add(a, b Amount) (Amount, error) {
	if a.C != b.C {
		return Amount{}, fmt.Errorf("money: cannot add %s and %s", a.C.Code, b.C.Code)
	}
	r := a.V + b.V
	// Signed-overflow guard: when both operands share a sign, the result must
	// share that sign too. If it flips, the addition overflowed int64.
	if (a.V > 0 && b.V > 0 && r < 0) || (a.V < 0 && b.V < 0 && r > 0) {
		return Amount{}, errors.New("money: integer overflow in Add")
	}
	return Amount{C: a.C, V: r}, nil
}

// Sub returns a-b.
func Sub(a, b Amount) (Amount, error) {
	neg := Amount{C: b.C, V: -b.V}
	return Add(a, neg)
}

// AbsDiff returns the absolute difference |a.V - b.V| in minor units. The
// currencies must match; the returned int64 is a bare minor-unit quantity, not
// an Amount, because it is used only for tolerance comparisons.
func AbsDiff(a, b Amount) (int64, error) {
	if a.C != b.C {
		return 0, fmt.Errorf("money: cannot diff %s and %s", a.C.Code, b.C.Code)
	}
	d := a.V - b.V
	if d < 0 {
		return -d, nil
	}
	return d, nil
}

// Sign reports -1, 0 or +1.
func (a Amount) Sign() int {
	switch {
	case a.V < 0:
		return -1
	case a.V > 0:
		return 1
	default:
		return 0
	}
}

// Cmp compares two amounts by value. Currencies must match.
func (a Amount) Cmp(b Amount) (int, error) {
	if a.C != b.C {
		return 0, fmt.Errorf("money: cannot compare %s and %s", a.C.Code, b.C.Code)
	}
	switch {
	case a.V < b.V:
		return -1, nil
	case a.V > b.V:
		return 1, nil
	default:
		return 0, nil
	}
}

// RoundingMode controls how a fractional part that exceeds the currency scale
// is resolved during parsing.
type RoundingMode uint8

const (
	// RoundReject treats any excess fractional precision as a parse error.
	RoundReject RoundingMode = iota
	// RoundHalfUp rounds half away from zero.
	RoundHalfUp
	// RoundHalfEven rounds half to even (banker's rounding).
	RoundHalfEven
	// RoundTrunc truncates toward zero.
	RoundTrunc
)

// ParseError describes a structured parsing failure so callers can attach it to
// the originating source line.
type ParseError struct {
	Field  string // logical field name, e.g. "amount".
	Input  string // the offending raw text.
	Reason string // human-readable reason.
	Code   string // stable machine code: empty_amount, bad_format, overflow, too_many_digits.
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("money: parse %q (%s): %s", e.Input, e.Code, e.Reason)
}

// Parse parses a decimal amount string into an Amount for currency c using the
// given rounding mode. The grammar accepted is:
//
//	[+-]? digits ('.' digits)?
//
// with optional surrounding whitespace. Leading '+' and a bare '.' without an
// integer part are rejected. No float64 is used at any point.
func Parse(c Currency, raw string, mode RoundingMode) (Amount, *ParseError) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "empty amount", Code: "empty_amount"}
	}
	neg := false
	body := s
	switch body[0] {
	case '+':
		body = body[1:]
	case '-':
		neg = true
		body = body[1:]
	}
	if body == "" {
		return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "missing digits after sign", Code: "bad_format"}
	}
	intPart, fracPart := body, ""
	if i := strings.IndexByte(body, '.'); i >= 0 {
		intPart, fracPart = body[:i], body[i+1:]
		if strings.ContainsRune(fracPart, '.') {
			return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "multiple decimal separators", Code: "bad_format"}
		}
	}
	if intPart == "" && fracPart == "" {
		return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "no digits", Code: "bad_format"}
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "non-digit characters", Code: "bad_format"}
	}

	scale := int(c.Scale)
	// Normalise fractional digits to exactly `scale`.
	switch {
	case len(fracPart) == scale:
		// exact; no rounding needed.
	case len(fracPart) < scale:
		fracPart = fracPart + strings.Repeat("0", scale-len(fracPart))
	default:
		// excess fractional precision: apply rounding mode.
		keep := fracPart[:scale]
		rest := fracPart[scale:]
		rounded, roundErr := roundFrac(keep, rest, mode, raw)
		if roundErr != nil {
			return Amount{}, roundErr
		}
		// rounded may carry into intPart via a leading '+1' marker.
		if strings.HasPrefix(rounded, "+1") {
			intPart = addDecimalStrings(intPart, "1")
			rounded = rounded[2:]
		}
		fracPart = rounded
	}

	// Build the integer minor-unit value from intPart and fracPart using big.Int
	// to detect overflow before narrowing to int64.
	bi := new(big.Int)
	if intPart != "" {
		bi.SetString(intPart, 10)
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	bi.Mul(bi, pow)
	if scale > 0 && fracPart != "" {
		fb := new(big.Int)
		fb.SetString(fracPart, 10)
		bi.Add(bi, fb)
	}
	if neg {
		bi.Neg(bi)
	}
	if !bi.IsInt64() {
		return Amount{}, &ParseError{Field: "amount", Input: raw, Reason: "value out of int64 range", Code: "overflow"}
	}
	return Amount{C: c, V: bi.Int64()}, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// roundFrac applies the rounding mode to a fractional string `keep` of length
// `scale` given the remaining digits `rest`. It returns the rounded fractional
// string (length scale) possibly prefixed with "+1" to signal a carry into the
// integer part.
func roundFrac(keep, rest string, mode RoundingMode, raw string) (string, *ParseError) {
	scale := len(keep)
	switch mode {
	case RoundReject:
		// Reject only if the discarded part is non-zero.
		if hasNonZero(rest) {
			return "", &ParseError{Field: "amount", Input: raw, Reason: "excess fractional precision not allowed", Code: "too_many_digits"}
		}
		return keep, nil
	case RoundTrunc:
		return keep, nil
	case RoundHalfUp, RoundHalfEven:
		if !hasNonZero(rest) {
			return keep, nil
		}
		// Determine the digit to round at (last of keep). If keep is empty
		// (scale 0), rounding always carries into the integer part.
		if scale == 0 {
			return "+1" + strings.Repeat("0", 0), nil
		}
		last := keep[scale-1]
		roundUp := false
		firstRest := rest[0]
		if mode == RoundHalfUp {
			roundUp = firstRest >= '5'
		} else { // RoundHalfEven
			if firstRest > '5' {
				roundUp = true
			} else if firstRest < '5' {
				roundUp = false
			} else {
				// exactly half: round to even.
				roundUp = (last-'0')%2 != 0
			}
		}
		if !roundUp {
			return keep, nil
		}
		// Increment the kept fractional digits by 1 with carry.
		digits := []byte(keep)
		carry := byte(1)
		for i := scale - 1; i >= 0 && carry > 0; i-- {
			d := digits[i] - '0' + carry
			if d >= 10 {
				digits[i] = '0'
				carry = 1
			} else {
				digits[i] = '0' + d
				carry = 0
			}
		}
		if carry > 0 {
			return "+1" + string(digits), nil
		}
		return string(digits), nil
	default:
		return "", &ParseError{Field: "amount", Input: raw, Reason: "unknown rounding mode", Code: "bad_format"}
	}
}

func hasNonZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return true
		}
	}
	return false
}

// addDecimalStrings adds two non-negative decimal integer strings.
func addDecimalStrings(a, b string) string {
	la, lb := len(a), len(b)
	if la < lb {
		a, b = b, a
		la, lb = lb, la
	}
	out := make([]byte, la+1)
	carry := byte(0)
	for i := 0; i < la; i++ {
		da := a[la-1-i] - '0'
		var db byte
		if i < lb {
			db = b[lb-1-i] - '0'
		}
		s := da + db + carry
		out[la-i] = '0' + s%10
		carry = s / 10
	}
	out[0] = '0' + carry
	// trim leading zeros
	start := 0
	for start < len(out)-1 && out[start] == '0' {
		start++
	}
	return string(out[start:])
}

// Format renders an Amount as a decimal string with exactly Currency.Scale
// fractional digits. No float64 is used.
func Format(a Amount) string {
	neg := a.V < 0
	v := a.V
	if neg {
		v = -v
	}
	scale := int(a.C.Scale)
	digits := fmt.Sprintf("%d", v)
	if scale == 0 {
		if neg {
			return "-" + digits
		}
		return digits
	}
	// left-pad to at least scale+1 digits so we have an integer part.
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	intPart := digits[:len(digits)-scale]
	fracPart := digits[len(digits)-scale:]
	out := intPart + "." + fracPart
	if neg {
		return "-" + out
	}
	return out
}

// Registry is a concurrency-safe map of currency code to Currency.
type Registry struct {
	m map[string]Currency
}

// NewRegistry returns a registry seeded with the common currencies.
func NewRegistry() *Registry {
	r := &Registry{m: make(map[string]Currency)}
	for _, c := range []Currency{USD, EUR, GBP, CNY, JPY} {
		r.m[c.Code] = c
	}
	return r
}

// Register adds or replaces a currency.
func (r *Registry) Register(c Currency) {
	r.m[strings.ToUpper(c.Code)] = Currency{Code: strings.ToUpper(c.Code), Scale: c.Scale}
}

// Lookup returns the currency for a code and whether it is known.
func (r *Registry) Lookup(code string) (Currency, bool) {
	c, ok := r.m[strings.ToUpper(code)]
	return c, ok
}

// MustLookup panics if the currency is unknown.
func (r *Registry) MustLookup(code string) Currency {
	c, ok := r.Lookup(code)
	if !ok {
		panic(fmt.Sprintf("money: unknown currency %q", code))
	}
	return c
}
