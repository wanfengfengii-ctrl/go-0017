package money

import (
	"strings"
	"testing"
)

func TestParseExact(t *testing.T) {
	cases := []struct {
		in   string
		cur  Currency
		want int64
	}{
		{"12.34", USD, 1234},
		{"0.01", USD, 1},
		{"100", USD, 10000},
		{"100.", USD, 10000},
		{".05", USD, 5},
		{"-12.34", USD, -1234},
		{"+12.34", USD, 1234},
		{"  12.34  ", USD, 1234},
		{"1234", JPY, 1234},
		{"12", JPY, 12},
		{"0", USD, 0},
		{"-0", USD, 0},
	}
	for _, tc := range cases {
		got, err := Parse(tc.cur, tc.in, RoundReject)
		if err != nil {
			t.Fatalf("Parse(%q,%s): unexpected error %v", tc.in, tc.cur.Code, err)
		}
		if got.V != tc.want {
			t.Errorf("Parse(%q,%s) = %d, want %d", tc.in, tc.cur.Code, got.V, tc.want)
		}
	}
}

func TestParseRejectsExcessDigits(t *testing.T) {
	_, err := Parse(USD, "12.345", RoundReject)
	if err == nil {
		t.Fatal("expected too_many_digits error")
	}
	if err.Code != "too_many_digits" {
		t.Fatalf("expected too_many_digits, got %s", err.Code)
	}
}

func TestParseRounding(t *testing.T) {
	cases := []struct {
		in   string
		mode RoundingMode
		want int64
	}{
		{"12.345", RoundHalfUp, 1235},
		{"12.344", RoundHalfUp, 1234},
		{"12.345", RoundTrunc, 1234},
		{"12.345", RoundHalfEven, 1234}, // 4 is even
		{"12.355", RoundHalfEven, 1236}, // 6 is even
		{"12.355", RoundHalfUp, 1236},
		{"0.005", RoundHalfUp, 1},
		{"0.005", RoundHalfEven, 0}, // 0 is even
		{"9.995", RoundHalfUp, 1000},
	}
	for _, tc := range cases {
		got, err := Parse(USD, tc.in, tc.mode)
		if err != nil {
			t.Fatalf("Parse(%q,%v): %v", tc.in, tc.mode, err)
		}
		if got.V != tc.want {
			t.Errorf("Parse(%q,%v) = %d, want %d", tc.in, tc.mode, got.V, tc.want)
		}
	}
}

func TestParseRoundingCarry(t *testing.T) {
	// 9.999 with half-up should carry into 10.00 = 1000 minor.
	got, err := Parse(USD, "9.999", RoundHalfUp)
	if err != nil {
		t.Fatal(err)
	}
	if got.V != 1000 {
		t.Errorf("got %d, want 1000", got.V)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{"", "abc", "1.2.3", "--1", "+", "-", "1e3", "1,000", "12.3a"}
	for _, in := range bad {
		_, err := Parse(USD, in, RoundReject)
		if err == nil {
			t.Errorf("Parse(%q): expected error", in)
		}
	}
}

func TestFormat(t *testing.T) {
	cases := []struct {
		a    Amount
		want string
	}{
		{FromMinor(USD, 1234), "12.34"},
		{FromMinor(USD, 5), "0.05"},
		{FromMinor(USD, 100), "1.00"},
		{FromMinor(USD, 0), "0.00"},
		{FromMinor(USD, -1234), "-12.34"},
		{FromMinor(USD, -5), "-0.05"},
		{FromMinor(JPY, 1234), "1234"},
		{FromMinor(JPY, 0), "0"},
		{FromMinor(JPY, -12), "-12"},
		{FromMinor(USD, 100000), "1000.00"},
	}
	for _, tc := range cases {
		got := Format(tc.a)
		if got != tc.want {
			t.Errorf("Format(%+v) = %q, want %q", tc.a, got, tc.want)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 5, 99, 100, 12345, -12345, 999999, 1000000} {
		a := FromMinor(USD, v)
		s := Format(a)
		b, err := Parse(USD, s, RoundReject)
		if err != nil {
			t.Fatalf("Parse(%q): %v", s, err)
		}
		if b.V != v {
			t.Errorf("round trip %d -> %q -> %d", v, s, b.V)
		}
	}
}

func TestArith(t *testing.T) {
	a := FromMinor(USD, 1234)
	b := FromMinor(USD, 566)
	s, err := Add(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if s.V != 1800 {
		t.Errorf("Add = %d, want 1800", s.V)
	}
	d, err := Sub(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if d.V != 668 {
		t.Errorf("Sub = %d, want 668", d.V)
	}
	diff, err := AbsDiff(a, b)
	if err != nil {
		t.Fatal(err)
	}
	if diff != 668 {
		t.Errorf("AbsDiff = %d, want 668", diff)
	}
	// currency mismatch
	_, err = Add(FromMinor(USD, 1), FromMinor(EUR, 1))
	if err == nil {
		t.Fatal("expected currency mismatch")
	}
}

func TestToleranceBoundary(t *testing.T) {
	abs := NewAbsTolerance(5) // 5 minor units
	// within: diff 0..5 -> true
	for diff := int64(0); diff <= 5; diff++ {
		if !abs.Within(diff, 1000) {
			t.Errorf("abs.Within(%d) = false, want true (boundary inclusive)", diff)
		}
	}
	// beyond: diff 6 -> false
	if abs.Within(6, 1000) {
		t.Errorf("abs.Within(6) = true, want false (one unit beyond)")
	}
	// CompareTolerance at boundary
	if got := CompareTolerance(abs, 5, 1000); got != 0 {
		t.Errorf("CompareTolerance(boundary) = %d, want 0", got)
	}
	if got := CompareTolerance(abs, 6, 1000); got != 1 {
		t.Errorf("CompareTolerance(6) = %d, want 1", got)
	}
	if got := CompareTolerance(abs, 4, 1000); got != -1 {
		t.Errorf("CompareTolerance(4) = %d, want -1", got)
	}
}

func TestTolerancePctBoundary(t *testing.T) {
	// 0.5% = 50 bps. base=1000 minor => boundary at 5 minor (1000*50/10000=5).
	pct := NewPctTolerance(50)
	for diff := int64(0); diff <= 5; diff++ {
		if !pct.Within(diff, 1000) {
			t.Errorf("pct.Within(%d) = false, want true", diff)
		}
	}
	if pct.Within(6, 1000) {
		t.Errorf("pct.Within(6) = true, want false")
	}
}

func TestToleranceCombinedBoundary(t *testing.T) {
	// Both abs=5 and pct=50bps (boundary 5 at base 1000). Combined requires both.
	abs5 := int64(5)
	bps := int64(50)
	tol := Tolerance{Abs: &abs5, PctBps: &bps}
	// exactly at both boundaries: diff=5, base=1000 -> both satisfied -> match
	if !tol.Within(5, 1000) {
		t.Errorf("combined boundary diff=5 should match")
	}
	// exceed one: diff=6 fails both
	if tol.Within(6, 1000) {
		t.Errorf("combined diff=6 should not match")
	}
	// pct boundary smaller than abs: base=200 => pct boundary=1, abs=5 -> within needs <=1
	if tol.Within(2, 200) {
		t.Errorf("combined at base=200, diff=2 should fail pct (boundary 1)")
	}
	if !tol.Within(1, 200) {
		t.Errorf("combined at base=200, diff=1 should match (pct boundary 1)")
	}
}

func TestToleranceString(t *testing.T) {
	s := NewAbsTolerance(5).String()
	if !strings.Contains(s, "abs=5") {
		t.Errorf("unexpected tolerance string %q", s)
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	c, ok := r.Lookup("usd")
	if !ok || c.Code != "USD" || c.Scale != 2 {
		t.Fatalf("lookup usd: %+v ok=%v", c, ok)
	}
	_, ok = r.Lookup("XYZ")
	if ok {
		t.Fatal("XYZ should be unknown")
	}
	r.Register(Currency{Code: "xyz", Scale: 3})
	c, ok = r.Lookup("XYZ")
	if !ok || c.Scale != 3 {
		t.Fatalf("registered xyz: %+v ok=%v", c, ok)
	}
}

func TestNoFloat64(t *testing.T) {
	// Sanity guard: the package must not mention float64 in its rounding path.
	// This is a weak heuristic test; the real guarantee is the implementation.
	a, err := Parse(USD, "9999999999.99", RoundHalfUp)
	if err != nil {
		t.Fatal(err)
	}
	if a.V != 999999999999 {
		t.Errorf("large amount parse = %d, want 999999999999", a.V)
	}
}
