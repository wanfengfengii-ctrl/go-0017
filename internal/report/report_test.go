package report

import (
	"bytes"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

func mkRec(id string, role model.Role, ext, stable string, amount int64, cur money.Currency, ts time.Time) *model.Record {
	return &model.Record{
		ID:         id,
		Role:       role,
		ExternalID: ext,
		StableKey:  stable,
		Amount:     money.FromMinor(cur, amount),
		Timestamp:  ts,
	}
}

func sampleReport(t *testing.T) *Report {
	t.Helper()
	bt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	byID := map[string]*model.Record{
		"r1": mkRec("r1", model.RoleInternal, "I1", "internal|I1", 1000, money.USD, bt),
		"r2": mkRec("r2", model.RoleProcessor, "P1", "processor|P1", 1000, money.USD, bt),
		"r3": mkRec("r3", model.RoleInternal, "I2", "internal|I2", 500, money.USD, bt), // standalone
	}
	matches := []*model.MatchGroup{
		{ID: "mg_a", Type: model.MatchExactID, Currency: "USD", AmountMinor: 1000, RecordIDs: []string{"r1", "r2"}, ScoreTuple: []int64{0, 0, 0, 0}},
	}
	discreps := []*model.Discrepancy{
		{ID: "disc_x", Type: model.DiscMissingProcessor, Currency: "USD", Evidence: []model.Evidence{{RecordID: "r3", AmountMinor: 500}}},
	}
	return Build("run1", bt, 1, []string{"b1"}, matches, discreps, byID)
}

func TestReportDeterminism(t *testing.T) {
	r1 := sampleReport(t)
	for i := 0; i < 5; i++ {
		r2 := sampleReport(t)
		j1, _ := r1.JSON()
		j2, _ := r2.JSON()
		if !bytes.Equal(j1, j2) {
			t.Fatalf("report JSON not deterministic:\n%s\n%s", j1, j2)
		}
		c1, _ := r1.CSV()
		c2, _ := r2.CSV()
		if !bytes.Equal(c1, c2) {
			t.Fatalf("report CSV not deterministic")
		}
		if r1.ContentDigest() != r2.ContentDigest() {
			t.Fatalf("digest not deterministic")
		}
	}
}

func TestReportSorting(t *testing.T) {
	// Discrepancies must be sorted by currency, then type, then sort key.
	bt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	byID := map[string]*model.Record{
		"a": mkRec("a", model.RoleInternal, "A", "internal|A", 100, money.USD, bt),
		"b": mkRec("b", model.RoleInternal, "B", "internal|B", 200, money.EUR, bt),
		"c": mkRec("c", model.RoleInternal, "C", "internal|C", 300, money.USD, bt),
	}
	discreps := []*model.Discrepancy{
		{ID: "d3", Type: model.DiscAmountMismatch, Currency: "USD", Evidence: []model.Evidence{{RecordID: "c"}}},
		{ID: "d1", Type: model.DiscMissingProcessor, Currency: "USD", Evidence: []model.Evidence{{RecordID: "a"}}},
		{ID: "d2", Type: model.DiscMissingProcessor, Currency: "EUR", Evidence: []model.Evidence{{RecordID: "b"}}},
	}
	r := Build("run", bt, 1, nil, nil, discreps, byID)
	// Expected order: EUR/missing (d2), USD/amount (d3), USD/missing (d1).
	want := []string{"d2", "d3", "d1"}
	for i, w := range want {
		if r.Discrepancies[i].ID != w {
			t.Errorf("pos %d = %s, want %s (full order: %v)", i, r.Discrepancies[i].ID, w, ids(r.Discrepancies))
		}
	}
}

func TestReportSummary(t *testing.T) {
	r := sampleReport(t)
	if r.Summary.MatchedCount != 1 {
		t.Errorf("matched count = %d, want 1", r.Summary.MatchedCount)
	}
	if r.Summary.MatchedAmountMinor != 1000 {
		t.Errorf("matched amount = %d, want 1000", r.Summary.MatchedAmountMinor)
	}
	if r.Summary.DiscrepancyCounts[model.DiscMissingProcessor] != 1 {
		t.Errorf("missing count = %d, want 1", r.Summary.DiscrepancyCounts[model.DiscMissingProcessor])
	}
}

func TestReportFields(t *testing.T) {
	r := sampleReport(t)
	if r.RuleRevision != 1 {
		t.Errorf("rule revision = %d", r.RuleRevision)
	}
	if len(r.BatchIDs) != 1 || r.BatchIDs[0] != "b1" {
		t.Errorf("batch ids = %v", r.BatchIDs)
	}
	if r.RunID != "run1" {
		t.Errorf("run id = %s", r.RunID)
	}
	if len(r.MatchGroups) != 1 {
		t.Fatalf("match groups = %d", len(r.MatchGroups))
	}
	if len(r.MatchGroups[0].Members) != 2 {
		t.Errorf("members = %d", len(r.MatchGroups[0].Members))
	}
}

func ids(dvs []DiscrepancyView) []string {
	out := make([]string, len(dvs))
	for i, d := range dvs {
		out[i] = d.ID
	}
	return out
}
