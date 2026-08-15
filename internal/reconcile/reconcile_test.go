package reconcile

import (
	"fmt"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

var bt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func mkRec(role model.Role, ext, bid string, amount int64, cur money.Currency) *model.Record {
	return &model.Record{
		ID:         "rec_" + string(role) + "_" + ext,
		SourceID:   string(role),
		Role:       role,
		ExternalID: ext,
		BusinessID: bid,
		Amount:     money.FromMinor(cur, amount),
		Fee:        money.Zero(cur),
		Direction:  model.DirectionCredit,
		Timestamp:  bt,
		StableKey:  string(role) + "|" + ext + "|" + fmt.Sprintf("%d", amount),
	}
}

func baseRuleSet() *model.RuleSet {
	return &model.RuleSet{
		Revision: 1,
		Rules: []*model.Rule{
			{
				ID:           "r1",
				Name:         "main",
				AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank},
				BusinessIDFields: map[model.Role]string{
					model.RoleInternal:  model.FieldBusinessID,
					model.RoleProcessor: model.FieldBusinessID,
					model.RoleBank:      model.FieldBusinessID,
				},
				AmountTolerance: money.NewAbsTolerance(5),
				FeeTolerance:    money.NewAbsTolerance(0),
				TimeWindow:      int64(60 * time.Second),
				DupPolicy:       map[model.DupType]model.DupAction{model.DupExactKey: model.DupExclude},
			},
		},
	}
}

func TestDeterminismAcrossWorkers(t *testing.T) {
	snap := &Snapshot{
		RunID: "run1",
		Records: []*model.Record{
			mkRec(model.RoleInternal, "I1", "B1", 1000, money.USD),
			mkRec(model.RoleProcessor, "P1", "B1", 1000, money.USD),
			mkRec(model.RoleBank, "K1", "B1", 1000, money.USD),
			mkRec(model.RoleInternal, "I2", "", 500, money.EUR),
			mkRec(model.RoleProcessor, "P2", "", 500, money.EUR),
		},
		RuleSet: baseRuleSet(),
	}
	c := NewCoordinator(bt)
	baseline := c.Run(snap, 1)
	for _, w := range []int{1, 2, 4, 8} {
		r := c.Run(snap, w)
		if !sameGroups(baseline.MatchGroups, r.MatchGroups) {
			t.Errorf("workers=%d: match groups differ", w)
		}
		if !sameDiscs(baseline.Discrepancies, r.Discrepancies) {
			t.Errorf("workers=%d: discrepancies differ", w)
		}
		if !sameDups(baseline.Duplicates, r.Duplicates) {
			t.Errorf("workers=%d: duplicates differ", w)
		}
	}
}

func TestSnapshotIsolation(t *testing.T) {
	snap := &Snapshot{
		RunID: "run1",
		Records: []*model.Record{
			mkRec(model.RoleInternal, "I1", "B1", 1000, money.USD),
			mkRec(model.RoleProcessor, "P1", "B1", 1000, money.USD),
		},
		RuleSet: baseRuleSet(),
	}
	c := NewCoordinator(bt)
	r1 := c.Run(snap, 2)
	// A new record arrives after the snapshot was frozen; it must not appear in
	// r1.
	snap2 := &Snapshot{
		RunID:   "run2",
		Records: append(snap.Records, mkRec(model.RoleBank, "LATE", "B1", 1000, money.USD)),
		RuleSet: baseRuleSet(),
	}
	r2 := c.Run(snap2, 2)
	if len(r1.MatchGroups) == len(r2.MatchGroups) && r1.MatchGroups[0].ID == r2.MatchGroups[0].ID {
		// r1 had 2 records (1 group), r2 had 3 (still 1 exact-id group but
		// different members => different id). They must differ.
	}
	// r1 group has 2 members; r2 group has 3 members.
	if len(r1.MatchGroups) > 0 && len(r1.MatchGroups[0].RecordIDs) != 2 {
		t.Errorf("r1 group members = %d, want 2", len(r1.MatchGroups[0].RecordIDs))
	}
	if len(r2.MatchGroups) > 0 && len(r2.MatchGroups[0].RecordIDs) != 3 {
		t.Errorf("r2 group members = %d, want 3", len(r2.MatchGroups[0].RecordIDs))
	}
	if len(r1.MatchGroups) > 0 && len(r2.MatchGroups) > 0 && r1.MatchGroups[0].ID == r2.MatchGroups[0].ID {
		t.Error("run1 and run2 group ids should differ (different inputs)")
	}
}

func TestDuplicateExclusion(t *testing.T) {
	// Two processor records with same external id -> one excluded.
	snap := &Snapshot{
		RunID: "run1",
		Records: []*model.Record{
			mkRec(model.RoleInternal, "I1", "B1", 1000, money.USD),
			mkRec(model.RoleProcessor, "P1", "B1", 1000, money.USD),
			mkRec(model.RoleProcessor, "P1", "B1", 1000, money.USD), // dup external id
		},
		RuleSet: baseRuleSet(),
	}
	// Give the duplicate a distinct ID so dedup can flag it.
	snap.Records[2].ID = "rec_processor_P1_dup"
	snap.Records[2].StableKey = "processor|P1|dup"
	c := NewCoordinator(bt)
	r := c.Run(snap, 1)
	if len(r.Duplicates) == 0 {
		t.Error("expected duplicate relation")
	}
	if !r.ExcludedIDs["rec_processor_P1_dup"] {
		t.Error("duplicate should be excluded")
	}
}

func sameGroups(a, b []*model.MatchGroup) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
		if !equalStrings(a[i].RecordIDs, b[i].RecordIDs) {
			return false
		}
	}
	return true
}
func sameDiscs(a, b []*model.Discrepancy) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}
func sameDups(a, b []*model.DuplicateRelation) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
