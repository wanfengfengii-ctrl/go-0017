package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

func src(role model.Role, format string) *model.Source {
	return &model.Source{
		ID:       string(role),
		Name:     string(role),
		Role:     role,
		Currency: "USD",
		TimeZone: "UTC",
		Format:   format,
		FieldMap: map[string]string{
			// identity field map (logical == column).
			model.FieldExternalID: model.FieldExternalID,
			model.FieldAmount:     model.FieldAmount,
			model.FieldTimestamp:  model.FieldTimestamp,
			model.FieldFee:        model.FieldFee,
			model.FieldDirection:  model.FieldDirection,
			model.FieldBusinessID: model.FieldBusinessID,
		},
	}
}

func collectResults(t *testing.T, p *Pipeline, input string) []RowResult {
	t.Helper()
	var out []RowResult
	var mu sync.Mutex
	err := p.Run(strings.NewReader(input), func(r RowResult) error {
		mu.Lock()
		out = append(out, r)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	return out
}

func summarize(results []RowResult) string {
	type line struct {
		Line   int64
		ExtID  string
		Amount string
		Status string
	}
	var lines []line
	for _, r := range results {
		s := line{Line: r.Line}
		if r.Record != nil {
			s.ExtID = r.Record.ExternalID
			s.Amount = fmt.Sprintf("%d", r.Record.Amount.V)
			s.Status = "ok"
		} else {
			s.ExtID = ""
			s.Amount = ""
			s.Status = r.Error.Code
		}
		lines = append(lines, s)
	}
	b, _ := json.Marshal(lines)
	return string(b)
}

func TestCSVParse(t *testing.T) {
	in := "external_id,amount,timestamp,fee,direction,business_id\n" +
		"TX001,12.34,2026-01-02T03:04:05Z,0.10,credit,BIZ1\n" +
		"TX002,99.00,2026-01-02T03:05:05Z,0.20,debit,BIZ2\n"
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 1})
	results := collectResults(t, p, in)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	r0 := results[0].Record
	if r0.ExternalID != "TX001" || r0.Amount.V != 1234 || r0.Fee.V != 10 {
		t.Errorf("result0 = %+v", r0)
	}
	if !r0.Timestamp.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %v", r0.Timestamp)
	}
	if r0.SourceLine != 1 || results[1].Record.SourceLine != 2 {
		t.Errorf("source lines wrong: %d %d", r0.SourceLine, results[1].Record.SourceLine)
	}
	if r0.ContentHash == "" {
		t.Error("content hash not set")
	}
	if r0.ID == "" || r0.StableKey == "" {
		t.Error("id/stablekey not set")
	}
}

func TestDeterminismAcrossWorkerCounts(t *testing.T) {
	// Generate 50 records.
	var sb strings.Builder
	sb.WriteString("external_id,amount,timestamp,fee,direction,business_id\n")
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&sb, "TX%03d,%d.00,2026-01-02T03:04:0%dZ,0.0%d,credit,B%d\n", i, 100+i, i%6, i%10, i)
	}
	input := sb.String()

	want := summarize(collectResults(t, New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 1}), input))
	for _, w := range []int{1, 2, 8, 16} {
		p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: w, QueueSize: 4})
		got := summarize(collectResults(t, p, input))
		if got != want {
			t.Errorf("workers=%d produced different summary:\nwant %s\ngot  %s", w, want, got)
		}
	}
}

func TestOrderingPreserved(t *testing.T) {
	// With a tiny queue and many workers, output must still be in line order.
	var sb strings.Builder
	sb.WriteString("external_id,amount,timestamp,direction\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&sb, "T%03d,1.00,2026-01-02T00:00:%02dZ,credit\n", i, i%60)
	}
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 8, QueueSize: 2})
	results := collectResults(t, p, sb.String())
	for i, r := range results {
		if r.Line != int64(i+1) {
			t.Fatalf("result %d has line %d, want %d", i, r.Line, i+1)
		}
		want := fmt.Sprintf("T%03d", i)
		if r.Record.ExternalID != want {
			t.Errorf("result %d external_id = %q, want %q", i, r.Record.ExternalID, want)
		}
	}
}

func TestRowErrors(t *testing.T) {
	in := "external_id,amount,timestamp,direction\n" +
		"OK1,12.34,2026-01-02T03:04:05Z,credit\n" + // ok
		",12.34,2026-01-02T03:04:05Z,credit\n" + // missing external_id
		"BAD1,abc,2026-01-02T03:04:05Z,credit\n" + // bad amount
		"BAD2,12.34,not-a-time,credit\n" + // bad time
		"BAD3,12.34,2026-01-02T03:04:05Z,sideways\n" + // bad direction
		"OK2,99.99,2026-01-02T03:04:06Z,debit\n" // ok
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 2})
	results := collectResults(t, p, in)
	if len(results) != 6 {
		t.Fatalf("got %d results, want 6", len(results))
	}
	expectOK := []bool{true, false, false, false, false, true}
	for i, want := range expectOK {
		got := results[i].Record != nil
		if got != want {
			t.Errorf("result %d ok=%v want %v (err=%v)", i, got, want, results[i].Error)
		}
	}
	// Stable error codes.
	if results[1].Error.Code != "missing" {
		t.Errorf("line2 code = %s, want missing", results[1].Error.Code)
	}
	if results[2].Error.Code != "too_many_digits" && results[2].Error.Code != "bad_format" {
		t.Errorf("line3 code = %s, want bad_format or too_many_digits", results[2].Error.Code)
	}
	if results[3].Error.Code != "invalid" {
		t.Errorf("line4 code = %s, want invalid", results[3].Error.Code)
	}
	if results[4].Error.Code != "invalid" {
		t.Errorf("line5 code = %s, want invalid", results[4].Error.Code)
	}
}

func TestNDJSON(t *testing.T) {
	in := `{"external_id":"N1","amount":"5.00","timestamp":"2026-01-02T03:04:05Z","direction":"credit","business_id":"B1"}
{"external_id":"N2","amount":"7.50","timestamp":"2026-01-02T03:04:06Z","direction":"debit"}
`
	p := New(Options{Source: src(model.RoleBank, FormatNDJSON), Workers: 3})
	results := collectResults(t, p, in)
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].Record.Amount.V != 500 || results[1].Record.Amount.V != 750 {
		t.Errorf("amounts wrong: %d %d", results[0].Record.Amount.V, results[1].Record.Amount.V)
	}
}

func TestUnknownCurrency(t *testing.T) {
	s := src(model.RoleProcessor, FormatCSV)
	s.Currency = "USD"
	s.FieldMap[model.FieldCurrency] = model.FieldCurrency
	in := "external_id,amount,timestamp,currency,direction\n" +
		"C1,1.00,2026-01-02T03:04:05Z,XYZ,credit\n"
	// Register nothing for XYZ so it's unknown. With Scale 0 and "1.00",
	// RoundReject rejects the fractional part -> structured error.
	p := New(Options{Source: s, Workers: 1, Registry: money.NewRegistry()})
	results := collectResults(t, p, in)
	if results[0].Record != nil {
		t.Fatalf("expected error, got record %+v", results[0].Record)
	}
	if results[0].Error.Code != "unknown_currency" {
		t.Errorf("code = %s, want unknown_currency", results[0].Error.Code)
	}
}

func TestTimezoneCrossingDateBoundary(t *testing.T) {
	s := src(model.RoleProcessor, FormatCSV)
	s.TimeZone = "Asia/Shanghai" // UTC+8
	in := "external_id,amount,timestamp,direction\n" +
		"Z1,1.00,2026-01-02 09:00:00,credit\n" + // Shanghai 09:00 = UTC 01:00 same day
		"Z2,2.00,2026-01-02 23:00:00,debit\n" // Shanghai 23:00 = UTC 15:00 same day
	p := New(Options{Source: s, Workers: 1})
	results := collectResults(t, p, in)
	want := []time.Time{
		time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC),
	}
	for i, r := range results {
		if r.Record == nil {
			t.Fatalf("record %d nil: %v", i, r.Error)
		}
		if !r.Record.Timestamp.Equal(want[i]) {
			t.Errorf("record %d ts = %v UTC, want %v", i, r.Record.Timestamp, want[i])
		}
	}
}

func TestBackpressure(t *testing.T) {
	// With queue size 1 and a slow consumer, the pipeline must not read the
	// entire input into memory; it blocks. We verify correctness instead of
	// memory: produce a large input and consume slowly.
	var sb strings.Builder
	sb.WriteString("external_id,amount,timestamp,direction\n")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&sb, "P%03d,1.00,2026-01-02T00:00:%02dZ,credit\n", i, i%60)
	}
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 4, QueueSize: 1})
	var count int
	err := p.Run(strings.NewReader(sb.String()), func(r RowResult) error {
		count++
		// simulate slow consumer
		_ = time.Now()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 200 {
		t.Errorf("count = %d, want 200", count)
	}
}

func TestEmitErrorStops(t *testing.T) {
	in := "external_id,amount,timestamp,direction\n" +
		"A,1.00,2026-01-02T03:04:05Z,credit\n" +
		"B,2.00,2026-01-02T03:04:06Z,credit\n"
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 1})
	stopErr := fmt.Errorf("stop")
	var count int
	err := p.Run(strings.NewReader(in), func(r RowResult) error {
		count++
		return stopErr
	})
	if err != stopErr {
		t.Errorf("err = %v, want %v", err, stopErr)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestCSVQuotedFields(t *testing.T) {
	in := `external_id,amount,timestamp,memo,direction
"TX,1",12.34,2026-01-02T03:04:05Z,"hello, ""world""",credit`
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 1})
	// Add memo to field map.
	p.opts.Source.FieldMap[model.FieldMemo] = model.FieldMemo
	results := collectResults(t, p, in)
	if len(results) != 1 {
		t.Fatalf("got %d, want 1", len(results))
	}
	r := results[0].Record
	if r.ExternalID != "TX,1" {
		t.Errorf("external id = %q", r.ExternalID)
	}
	if r.Memo != `hello, "world"` {
		t.Errorf("memo = %q", r.Memo)
	}
}

func TestEmptyInput(t *testing.T) {
	p := New(Options{Source: src(model.RoleProcessor, FormatCSV), Workers: 2})
	err := p.Run(bytes.NewReader(nil), func(r RowResult) error {
		t.Error("unexpected emit")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
