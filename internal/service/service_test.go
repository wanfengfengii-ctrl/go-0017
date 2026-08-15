package service

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
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

func TestServiceIsolatedInvalidRecordsAppearInReconciliationReportsAcrossWorkers(t *testing.T) {
	internalCSV := "external_id,amount,timestamp,direction,business_id\nI1,100.00,2026-01-02T03:04:05Z,credit,B1\nI2,not-a-number,2026-01-02T03:04:05Z,credit,B2\n"
	processorCSV := "external_id,amount,timestamp,direction,business_id\nP1,100.00,2026-01-02T03:04:05Z,credit,B1\n"

	var baselineJSON, baselineCSV []byte
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			svc := testService(t)
			WithWorkers(workers)(svc)

			internalBatch, err := svc.SubmitAndCommit("internal", "internal-key", "csv", strings.NewReader(internalCSV), model.RowPolicyIsolate)
			if err != nil {
				t.Fatal(err)
			}
			if internalBatch.Status != model.BatchCommitted {
				t.Fatalf("batch status = %s, want %s", internalBatch.Status, model.BatchCommitted)
			}
			if internalBatch.Summary == nil || internalBatch.Summary.ValidCount != 1 || internalBatch.Summary.InvalidCount != 1 {
				t.Fatalf("batch summary = %+v, want valid=1 invalid=1", internalBatch.Summary)
			}

			processorBatch, err := svc.SubmitAndCommit("processor", "processor-key", "csv", strings.NewReader(processorCSV), model.RowPolicyIsolate)
			if err != nil {
				t.Fatal(err)
			}
			run, err := svc.StartRun("isolated-run", []string{internalBatch.ID, processorBatch.ID}, 1, workers)
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != model.RunSucceeded {
				t.Fatalf("run status = %s, want %s", run.Status, model.RunSucceeded)
			}
			if len(run.MatchGroups) != 1 || len(run.MatchGroups[0].RecordIDs) != 2 {
				t.Fatalf("match groups = %+v, want one two-record group", run.MatchGroups)
			}
			if len(run.Discrepancies) != 1 {
				t.Fatalf("discrepancies = %d, want 1", len(run.Discrepancies))
			}
			disc := run.Discrepancies[0]
			if disc.Type != model.DiscInvalidRecord || disc.Note == "" {
				t.Fatalf("discrepancy = %+v, want invalid_record with reason", disc)
			}
			if len(disc.Evidence) != 1 || disc.Evidence[0].RecordID != "rec_invalid_internal_2" {
				t.Fatalf("evidence = %+v, want isolated source record", disc.Evidence)
			}
			if run.Summary == nil || run.Summary.DiscrepancyCounts[model.DiscInvalidRecord] != 1 {
				t.Fatalf("report summary = %+v, want invalid_record=1", run.Summary)
			}

			jsonData, err := svc.ExportReport(run.ID, "json")
			if err != nil {
				t.Fatal(err)
			}
			var jsonReport struct {
				Discrepancies []struct {
					Type     model.DiscType   `json:"type"`
					Note     string           `json:"note"`
					Evidence []model.Evidence `json:"evidence"`
				} `json:"discrepancies"`
				Summary *model.ReportSummary `json:"summary"`
			}
			if err := json.Unmarshal(jsonData, &jsonReport); err != nil {
				t.Fatal(err)
			}
			if len(jsonReport.Discrepancies) != 1 || jsonReport.Discrepancies[0].Type != model.DiscInvalidRecord || jsonReport.Discrepancies[0].Note != disc.Note || len(jsonReport.Discrepancies[0].Evidence) != 1 {
				t.Fatalf("JSON discrepancies = %+v, want the isolated record and reason", jsonReport.Discrepancies)
			}
			if jsonReport.Summary == nil || jsonReport.Summary.DiscrepancyCounts[model.DiscInvalidRecord] != 1 {
				t.Fatalf("JSON summary = %+v, want invalid_record=1", jsonReport.Summary)
			}

			csvData, err := svc.ExportReport(run.ID, "csv")
			if err != nil {
				t.Fatal(err)
			}
			reader := csv.NewReader(strings.NewReader(string(csvData)))
			reader.FieldsPerRecord = -1
			rows, err := reader.ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 3 || len(rows[1]) != 6 || rows[1][1] != string(model.DiscInvalidRecord) || rows[1][3] != disc.Note || rows[1][4] != disc.Evidence[0].RecordID {
				t.Fatalf("CSV rows = %v, want one invalid_record row with evidence and reason", rows)
			}

			if baselineJSON == nil {
				baselineJSON = append([]byte(nil), jsonData...)
				baselineCSV = append([]byte(nil), csvData...)
				return
			}
			if !bytes.Equal(jsonData, baselineJSON) || !bytes.Equal(csvData, baselineCSV) {
				t.Fatal("reports differ across worker configurations")
			}
		})
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
