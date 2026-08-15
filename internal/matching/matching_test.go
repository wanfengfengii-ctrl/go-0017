package matching

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

var baseTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func mkRecord(role model.Role, ext, bid string, amount, fee int64, t time.Time) *model.Record {
	cur := money.USD
	return &model.Record{
		ID:         "rec_" + string(role) + "_" + ext,
		SourceID:   string(role),
		Role:       role,
		ExternalID: ext,
		BusinessID: bid,
		Amount:     money.FromMinor(cur, amount),
		Fee:        money.FromMinor(cur, fee),
		Direction:  model.DirectionCredit,
		Timestamp:  t,
		StableKey:  string(role) + "|" + ext + "|" + fmt.Sprintf("%d", amount),
	}
}

func absRule(allowed ...model.Role) *model.Rule {
	return &model.Rule{
		ID:           "rule1",
		Name:         "test",
		AllowedRoles: allowed,
		BusinessIDFields: map[model.Role]string{
			model.RoleInternal:  model.FieldBusinessID,
			model.RoleProcessor: model.FieldBusinessID,
			model.RoleBank:      model.FieldBusinessID,
		},
		AmountTolerance: money.NewAbsTolerance(5),
		FeeTolerance:    money.NewAbsTolerance(0),
		TimeWindow:      int64(60 * time.Second),
	}
}

func TestExactIDMatch(t *testing.T) {
	rule := absRule(model.RoleInternal, model.RoleProcessor, model.RoleBank)
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "BIZ1", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "BIZ1", 1000, 0, baseTime),
		mkRecord(model.RoleBank, "K1", "BIZ1", 1000, 0, baseTime),
	}
	e := NewEngine([]*model.Rule{rule}, nil, baseTime)
	res := e.Run(recs, nil)
	if len(res.Groups) != 1 {
		t.Fatalf("groups = %d, want 1: %+v", len(res.Groups), res.Groups)
	}
	g := res.Groups[0]
	if g.Type != model.MatchExactID {
		t.Errorf("type = %s, want exact_id", g.Type)
	}
	if len(g.RecordIDs) != 3 {
		t.Errorf("members = %d, want 3", len(g.RecordIDs))
	}
	if len(res.Discrepancies) != 0 {
		t.Errorf("discrepancies = %d, want 0: %+v", len(res.Discrepancies), res.Discrepancies)
	}
}

func TestToleranceBoundaryMatch(t *testing.T) {
	// absolute tolerance 5; diff exactly 5 -> match; diff 6 -> mismatch.
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	// matching case
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1005, 0, baseTime),
	}
	e := NewEngine([]*model.Rule{rule}, nil, baseTime)
	res := e.Run(recs, nil)
	if len(res.Groups) != 1 {
		t.Fatalf("boundary 5: groups = %d, want 1", len(res.Groups))
	}
	// beyond boundary by one unit -> amount_mismatch
	recs2 := []*model.Record{
		mkRecord(model.RoleInternal, "I2", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 1006, 0, baseTime),
	}
	e2 := NewEngine([]*model.Rule{rule}, nil, baseTime)
	res2 := e2.Run(recs2, nil)
	if len(res2.Groups) != 0 {
		t.Errorf("beyond boundary: groups = %d, want 0", len(res2.Groups))
	}
	found := false
	for _, d := range res2.Discrepancies {
		if d.Type == model.DiscAmountMismatch {
			found = true
		}
	}
	if !found {
		t.Errorf("beyond boundary: expected amount_mismatch, got %+v", discTypes(res2.Discrepancies))
	}
}

func TestPctToleranceBoundary(t *testing.T) {
	rule := &model.Rule{
		ID: "r", Name: "r", AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor},
		BusinessIDFields: map[model.Role]string{model.RoleInternal: model.FieldBusinessID, model.RoleProcessor: model.FieldBusinessID},
		AmountTolerance:  money.NewPctTolerance(50), // 0.5%
		TimeWindow:       int64(60 * time.Second),
	}
	// base 1000, 0.5% = 5 boundary. diff 5 -> match, diff 6 -> mismatch.
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1005, 0, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	if len(res.Groups) != 1 {
		t.Errorf("pct boundary match: groups = %d, want 1", len(res.Groups))
	}
	recs2 := []*model.Record{
		mkRecord(model.RoleInternal, "I2", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 1006, 0, baseTime),
	}
	res2 := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs2, nil)
	if len(res2.Groups) != 0 {
		t.Errorf("pct beyond: groups = %d, want 0", len(res2.Groups))
	}
}

func TestTimeWindowBoundary(t *testing.T) {
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(0) // exact amount
	// exactly 60s apart -> match (inclusive); 60s + 1ns -> mismatch.
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1000, 0, baseTime.Add(60*time.Second)),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	if len(res.Groups) != 1 {
		t.Errorf("time boundary: groups = %d, want 1", len(res.Groups))
	}
	recs2 := []*model.Record{
		mkRecord(model.RoleInternal, "I2", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 1000, 0, baseTime.Add(60*time.Second+1*time.Nanosecond)),
	}
	res2 := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs2, nil)
	if len(res2.Groups) != 0 {
		t.Errorf("time beyond: groups = %d, want 0", len(res2.Groups))
	}
}

func TestAmbiguous(t *testing.T) {
	// One internal record, two processor records with identical amounts/times.
	// Both tie for best -> ambiguous.
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(0)
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 1000, 0, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	if len(res.Groups) != 0 {
		t.Errorf("ambiguous: groups = %d, want 0 (must not pick arbitrarily)", len(res.Groups))
	}
	var amb *model.Discrepancy
	for _, d := range res.Discrepancies {
		if d.Type == model.DiscAmbiguous {
			amb = d
		}
	}
	if amb == nil {
		t.Fatalf("ambiguous: expected ambiguous discrepancy, got %+v", discTypes(res.Discrepancies))
	}
	// candidates must be sorted by stable key.
	cands := amb.Evidence[0].Candidates
	if len(cands) != 2 {
		t.Fatalf("candidates = %d, want 2", len(cands))
	}
	keys := []string{cands[0].StableKey, cands[1].StableKey}
	if !sort.StringsAreSorted(keys) {
		t.Errorf("candidates not sorted by stable key: %v", keys)
	}
}

func TestAmbiguousResolvesWithUniqueID(t *testing.T) {
	// Two processor candidates tie -> ambiguous. Adding a unique business id to
	// one via a new rule revision yields a unique exact-id match.
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(0)
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "BIZ9", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "BIZ9", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 1000, 0, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	// I1 and P1 share BIZ9 -> exact match; P2 standalone -> missing/ambiguous?
	exactCount := 0
	for _, g := range res.Groups {
		if g.Type == model.MatchExactID {
			exactCount++
		}
	}
	if exactCount != 1 {
		t.Errorf("unique-id rerun: exact groups = %d, want 1", exactCount)
	}
}

func TestOneToMany(t *testing.T) {
	rule := absRule(model.RoleBank, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(1) // small tolerance
	rule.OneToMany = &model.OneToManyRule{
		AnchorRole: model.RoleBank,
		ManyRole:   model.RoleProcessor,
		MaxMembers: 3,
	}
	rule.SearchLimit = 10000
	// bank 100; two processor 40 + 60 = 100 -> one_to_many match.
	recs := []*model.Record{
		mkRecord(model.RoleBank, "K1", "", 100, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 40, 0, baseTime),
		mkRecord(model.RoleProcessor, "P2", "", 60, 0, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	if len(res.Groups) != 1 {
		t.Fatalf("1:N: groups = %d, want 1: %+v", len(res.Groups), res.Groups)
	}
	g := res.Groups[0]
	if g.Type != model.MatchOneToMany {
		t.Errorf("type = %s, want one_to_many", g.Type)
	}
	if len(g.RecordIDs) != 3 {
		t.Errorf("members = %d, want 3", len(g.RecordIDs))
	}
}

func TestSearchLimitByCap(t *testing.T) {
	rule := absRule(model.RoleBank, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(1)
	rule.OneToMany = &model.OneToManyRule{
		AnchorRole: model.RoleBank, ManyRole: model.RoleProcessor, MaxMembers: 3,
	}
	rule.SearchLimit = 5 // very small cap
	// bank 100; many processor 10 each, 5 of them. No subset sums to 100 within
	// size <= 3, and the search cap is hit.
	recs := []*model.Record{
		mkRecord(model.RoleBank, "K1", "", 100, 0, baseTime),
	}
	for i := 0; i < 5; i++ {
		recs = append(recs, mkRecord(model.RoleProcessor, fmt.Sprintf("P%d", i), "", 10, 0, baseTime))
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	found := false
	for _, d := range res.Discrepancies {
		if d.Type == model.DiscSearchLimit {
			found = true
		}
	}
	if !found {
		t.Errorf("expected search_limit, got %+v", discTypes(res.Discrepancies))
	}
}

func TestMissingClassification(t *testing.T) {
	rule := absRule(model.RoleInternal, model.RoleProcessor, model.RoleBank)
	// standalone internal -> missing_processor
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	found := false
	for _, d := range res.Discrepancies {
		if d.Type == model.DiscMissingProcessor {
			found = true
		}
	}
	if !found {
		t.Errorf("expected missing_processor, got %+v", discTypes(res.Discrepancies))
	}
}

func TestDeterminismOfGroupIDs(t *testing.T) {
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(0)
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1000, 0, baseTime),
	}
	r1 := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	// reverse input order
	rev := []*model.Record{recs[1], recs[0]}
	r2 := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(rev, nil)
	if len(r1.Groups) != 1 || len(r2.Groups) != 1 {
		t.Fatalf("groups mismatch")
	}
	if r1.Groups[0].ID != r2.Groups[0].ID {
		t.Errorf("group id differs across input order: %s vs %s", r1.Groups[0].ID, r2.Groups[0].ID)
	}
}

func TestFeeMismatch(t *testing.T) {
	rule := absRule(model.RoleInternal, model.RoleProcessor)
	rule.AmountTolerance = money.NewAbsTolerance(0)
	rule.FeeTolerance = money.NewAbsTolerance(1)
	// amount equal, time aligned, fee differs by 5 (>1) -> fee_mismatch
	recs := []*model.Record{
		mkRecord(model.RoleInternal, "I1", "", 1000, 0, baseTime),
		mkRecord(model.RoleProcessor, "P1", "", 1000, 5, baseTime),
	}
	res := NewEngine([]*model.Rule{rule}, nil, baseTime).Run(recs, nil)
	found := false
	for _, d := range res.Discrepancies {
		if d.Type == model.DiscFeeMismatch {
			found = true
		}
	}
	if !found {
		t.Errorf("expected fee_mismatch, got %+v", discTypes(res.Discrepancies))
	}
}

func discTypes(ds []*model.Discrepancy) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d.Type)
	}
	return out
}
