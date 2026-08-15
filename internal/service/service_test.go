package service

import (
	"strings"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
	"github.com/settlemesh/settlemesh/internal/store"
	"github.com/settlemesh/settlemesh/internal/testcontrol"
)

func fixedClock() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func testService(t *testing.T) *Service {
	t.Helper()
	s := store.New(fixedClock)
	svc := New(s, WithClock(fixedClock), WithWorkers(2))
	for _, role := range []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank} {
		svc.AddSource(&model.Source{
			ID: string(role), Name: string(role), Role: role, Currency: "USD", TimeZone: "UTC", Format: "csv",
			FieldMap: map[string]string{
				model.FieldExternalID: model.FieldExternalID,
				model.FieldAmount:     model.FieldAmount,
				model.FieldTimestamp:  model.FieldTimestamp,
				model.FieldFee:        model.FieldFee,
				model.FieldDirection:  model.FieldDirection,
				model.FieldBusinessID: model.FieldBusinessID,
			},
		})
	}
	abs5 := int64(5)
	zero := int64(0)
	svc.PutRuleSet(&model.RuleSet{Revision: 1, Rules: []*model.Rule{{
		ID:           "r1",
		AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank},
		BusinessIDFields: map[model.Role]string{
			model.RoleInternal:  model.FieldBusinessID,
			model.RoleProcessor: model.FieldBusinessID,
			model.RoleBank:      model.FieldBusinessID,
		},
		AmountTolerance: money.Tolerance{Abs: &abs5},
		FeeTolerance:    money.Tolerance{Abs: &zero},
		TimeWindow:      int64(60 * time.Second),
	}}})
	return svc
}

func TestServiceStartRun(t *testing.T) {
	svc := testService(t)
	internal := "external_id,amount,timestamp,direction,business_id\nI1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	proc := "external_id,amount,timestamp,direction,business_id\nP1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	bank := "external_id,amount,timestamp,direction,business_id\nK1,100.00,2026-01-02T03:04:05Z,credit,B1\n"

	bi, _ := svc.SubmitAndCommit("internal", "ki", "csv", strings.NewReader(internal), model.RowPolicyIsolate)
	bp, _ := svc.SubmitAndCommit("processor", "kp", "csv", strings.NewReader(proc), model.RowPolicyIsolate)
	bk, _ := svc.SubmitAndCommit("bank", "kb", "csv", strings.NewReader(bank), model.RowPolicyIsolate)

	run, err := svc.StartRun("run1", []string{bi.ID, bp.ID, bk.ID}, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != model.RunSucceeded {
		t.Fatalf("status = %s", run.Status)
	}
	if len(run.MatchGroups) != 1 {
		t.Errorf("groups = %d, want 1", len(run.MatchGroups))
	}
	if len(run.MatchGroups) > 0 && len(run.MatchGroups[0].RecordIDs) != 3 {
		t.Errorf("members = %d, want 3", len(run.MatchGroups[0].RecordIDs))
	}
}

func TestServiceIdempotency(t *testing.T) {
	svc := testService(t)
	csv := "external_id,amount,timestamp,direction\nI1,1.00,2026-01-02T03:04:05Z,credit\n"
	b1, err := svc.SubmitAndCommit("internal", "key1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := svc.SubmitAndCommit("internal", "key1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	if b1.ID != b2.ID {
		t.Errorf("same key should return same batch: %s vs %s", b1.ID, b2.ID)
	}
	if b2.Status != model.BatchCommitted {
		t.Errorf("status = %s, want committed (already committed)", b2.Status)
	}
}

func TestServiceRetransmitDifferentKey(t *testing.T) {
	svc := testService(t)
	csv := "external_id,amount,timestamp,direction\nI1,1.00,2026-01-02T03:04:05Z,credit\n"
	b1, _ := svc.SubmitAndCommit("internal", "key1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	b2, _ := svc.SubmitAndCommit("internal", "key2", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	if b1.ID == b2.ID {
		t.Fatal("different keys should create different batches")
	}
}

func TestServiceCancelRun(t *testing.T) {
	// Create a run via the store directly (no batches) so we can cancel before
	// it completes. StartRun completes synchronously, so we cancel a freshly
	// created running run instead.
	st := store.New(fixedClock)
	st.CreateRun(&model.Run{ID: "r", Status: model.RunRunning})
	svc2 := New(st, WithClock(fixedClock))
	run, err := svc2.CancelRun("r")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != model.RunCancelled {
		t.Errorf("status = %s, want cancelled", run.Status)
	}
}

func TestServiceFaultInjection(t *testing.T) {
	ctrl := testcontrol.New(fixedClock())
	s := store.New(fixedClock)
	svc := New(s, WithClock(fixedClock), WithControl(ctrl), WithWorkers(1))
	for _, role := range []model.Role{model.RoleInternal, model.RoleProcessor} {
		svc.AddSource(&model.Source{ID: string(role), Role: role, Currency: "USD", TimeZone: "UTC", Format: "csv",
			FieldMap: map[string]string{
				model.FieldExternalID: model.FieldExternalID, model.FieldAmount: model.FieldAmount,
				model.FieldTimestamp: model.FieldTimestamp, model.FieldDirection: model.FieldDirection,
				model.FieldBusinessID: model.FieldBusinessID,
			}})
	}
	svc.PutRuleSet(&model.RuleSet{Revision: 1, Rules: []*model.Rule{{
		ID: "r", AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor},
		BusinessIDFields: map[model.Role]string{model.RoleInternal: model.FieldBusinessID, model.RoleProcessor: model.FieldBusinessID},
		AmountTolerance:  money.NewAbsTolerance(5), TimeWindow: int64(60 * time.Second),
	}}})

	// Fault at staging commit: SubmitAndCommit should fail and the batch must
	// not be committable afterwards.
	ctrl.SetFault(testcontrol.FaultStagingCommit, testcontrol.ErrFaulted)
	csv := "external_id,amount,timestamp,direction\nI1,1.00,2026-01-02T03:04:05Z,credit\n"
	_, err := svc.SubmitAndCommit("internal", "k1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	if err == nil {
		t.Fatal("expected staging commit fault")
	}
	// Clear fault and retry with the SAME request key: it must succeed and
	// reuse the batch that was failed.
	ctrl.ClearFault(testcontrol.FaultStagingCommit)
	b, err := svc.SubmitAndCommit("internal", "k1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if b.Status != model.BatchCommitted {
		t.Errorf("retry status = %s, want committed", b.Status)
	}
}

func TestServiceExportReport(t *testing.T) {
	svc := testService(t)
	internal := "external_id,amount,timestamp,direction,business_id\nI1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	proc := "external_id,amount,timestamp,direction,business_id\nP1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	bi, _ := svc.SubmitAndCommit("internal", "ki", "csv", strings.NewReader(internal), model.RowPolicyIsolate)
	bp, _ := svc.SubmitAndCommit("processor", "kp", "csv", strings.NewReader(proc), model.RowPolicyIsolate)
	svc.StartRun("run1", []string{bi.ID, bp.ID}, 1, 2)

	jsonData, err := svc.ExportReport("run1", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonData), "match_groups") {
		t.Errorf("json report missing match_groups: %s", jsonData)
	}
	csvData, err := svc.ExportReport("run1", "csv")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csvData), "discrepancy_id") {
		t.Errorf("csv report missing header: %s", csvData)
	}
}

func TestServiceRecover(t *testing.T) {
	s := store.New(fixedClock)
	// Plant an unfinished batch and run.
	s.SubmitBatch(store.BatchRequest{SourceID: "x", IdempotencyKey: "k", FileDigest: "d"})
	s.CreateRun(&model.Run{ID: "r", Status: model.RunRunning})
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	// After recovery, batches/runs are terminal.
	for _, b := range s.ListBatches() {
		if b.Status != model.BatchFailed {
			t.Errorf("batch status = %s, want failed", b.Status)
		}
	}
	r, _ := s.GetRun("r")
	if r.Status != model.RunFailed {
		t.Errorf("run status = %s, want failed", r.Status)
	}
}

// TestServiceIsolateRecordsReported is a regression test for invalid records
// imported under RowPolicyIsolate: each isolated row must surface as an
// invalid_record discrepancy in the reconciliation result and in the exported
// report (JSON and CSV), and the aggregated summary must count them. The
// result must be identical for any reconcile worker count.
func TestServiceIsolateRecordsReported(t *testing.T) {
	svc := testService(t)
	// Each source contributes one valid row (shared business id B1 -> one
	// exact-id match group) and one invalid row that becomes an isolated
	// record under the isolate policy.
	internal := "external_id,amount,timestamp,direction,business_id\n" +
		"I1,100.00,2026-01-02T03:04:05Z,credit,B1\n" +
		"I2,,2026-01-02T03:04:05Z,credit,B2\n" // missing amount
	processor := "external_id,amount,timestamp,direction,business_id\n" +
		"P1,100.00,2026-01-02T03:04:05Z,credit,B1\n" +
		"P2,100.00,not-a-timestamp,credit,B2\n" // bad timestamp
	bank := "external_id,amount,timestamp,direction,business_id\n" +
		"K1,100.00,2026-01-02T03:04:05Z,credit,B1\n" +
		",100.00,2026-01-02T03:04:05Z,credit,B2\n" // missing external id

	bi, err := svc.SubmitAndCommit("internal", "ki", "csv", strings.NewReader(internal), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	bp, err := svc.SubmitAndCommit("processor", "kp", "csv", strings.NewReader(processor), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	bk, err := svc.SubmitAndCommit("bank", "kb", "csv", strings.NewReader(bank), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}

	// Batch submission: each committed batch accounts for one valid and one
	// isolated record.
	for _, b := range []*model.Batch{bi, bp, bk} {
		if b.Status != model.BatchCommitted {
			t.Fatalf("batch %s status = %s, want committed", b.ID, b.Status)
		}
		if b.Summary == nil {
			t.Fatalf("batch %s missing summary", b.ID)
		}
		if b.Summary.ValidCount != 1 {
			t.Errorf("batch %s valid_count = %d, want 1", b.ID, b.Summary.ValidCount)
		}
		if b.Summary.InvalidCount != 1 {
			t.Errorf("batch %s invalid_count = %d, want 1", b.ID, b.Summary.InvalidCount)
		}
	}

	batchIDs := []string{bi.ID, bp.ID, bk.ID}

	// Reconciliation run with a single worker as the baseline.
	baseline, err := svc.StartRun("run_iso_1", batchIDs, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Status != model.RunSucceeded {
		t.Fatalf("baseline status = %s, want succeeded", baseline.Status)
	}
	// Valid rows form one exact-id match group of three members.
	if len(baseline.MatchGroups) != 1 {
		t.Fatalf("baseline groups = %d, want 1", len(baseline.MatchGroups))
	}
	if len(baseline.MatchGroups[0].RecordIDs) != 3 {
		t.Errorf("baseline match members = %d, want 3", len(baseline.MatchGroups[0].RecordIDs))
	}
	// Each isolated record surfaces as an invalid_record discrepancy.
	if got := countByType(baseline.Discrepancies, model.DiscInvalidRecord); got != 3 {
		t.Fatalf("baseline invalid_record discrepancies = %d, want 3", got)
	}
	// Report aggregation: the summary counts the invalid records.
	if baseline.Summary == nil {
		t.Fatal("baseline summary is nil")
	}
	if got := baseline.Summary.DiscrepancyCounts[model.DiscInvalidRecord]; got != 3 {
		t.Errorf("summary invalid_record count = %d, want 3", got)
	}

	// Exported reports surface the invalid records in both formats.
	jsonData, err := svc.ExportReport("run_iso_1", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonData), "invalid_record") {
		t.Errorf("json report missing invalid_record: %s", jsonData)
	}
	csvData, err := svc.ExportReport("run_iso_1", "csv")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csvData), "invalid_record") {
		t.Errorf("csv report missing invalid_record: %s", csvData)
	}

	// Consistency across worker configurations: identical discrepancy and
	// match-group id sets for any reconcile worker count.
	wantDiscs := idSet(discIDs(baseline.Discrepancies))
	wantGroups := idSet(groupIDs(baseline.MatchGroups))
	runIDs := map[int]string{2: "run_iso_2", 4: "run_iso_4", 8: "run_iso_8"}
	for _, w := range []int{2, 4, 8} {
		run, err := svc.StartRun(runIDs[w], batchIDs, 1, w)
		if err != nil {
			t.Fatal(err)
		}
		if got := countByType(run.Discrepancies, model.DiscInvalidRecord); got != 3 {
			t.Errorf("workers=%d: invalid_record discrepancies = %d, want 3", w, got)
		}
		if !equalStringSet(wantDiscs, idSet(discIDs(run.Discrepancies))) {
			t.Errorf("workers=%d: discrepancy id set differs", w)
		}
		if !equalStringSet(wantGroups, idSet(groupIDs(run.MatchGroups))) {
			t.Errorf("workers=%d: match group id set differs", w)
		}
	}
}

func countByType(ds []*model.Discrepancy, dtype model.DiscType) int {
	n := 0
	for _, d := range ds {
		if d.Type == dtype {
			n++
		}
	}
	return n
}

func discIDs(ds []*model.Discrepancy) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.ID)
	}
	return out
}

func groupIDs(gs []*model.MatchGroup) []string {
	out := make([]string, 0, len(gs))
	for _, g := range gs {
		out = append(out, g.ID)
	}
	return out
}

func idSet(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func equalStringSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
