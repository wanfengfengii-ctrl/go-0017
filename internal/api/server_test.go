package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/store"
)

func newTestService(t *testing.T) *service.Service {
	t.Helper()
	s := store.New(func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) })
	svc := service.New(s)
	// Register sources.
	for _, role := range []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank} {
		svc.AddSource(&model.Source{
			ID:       string(role),
			Name:     string(role),
			Role:     role,
			Currency: "USD",
			TimeZone: "UTC",
			Format:   "csv",
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
	// Put a rule set.
	rs := &model.RuleSet{Revision: 1, Rules: []*model.Rule{{
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
	}}}
	svc.PutRuleSet(rs)
	return svc
}

func TestAPIFlow(t *testing.T) {
	svc := newTestService(t)
	srv := New(svc)

	// Submit internal feed.
	internalCSV := "external_id,amount,timestamp,direction,business_id\n" +
		"I1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	req := httptest.NewRequest("POST", "/batches?source_id=internal&format=csv&row_policy=isolate&idempotency_key=k1", strings.NewReader(internalCSV))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("submit internal: %d %s", rec.Code, rec.Body.String())
	}
	var internalBatch model.Batch
	json.Unmarshal(rec.Body.Bytes(), &internalBatch)

	// Submit processor feed.
	procCSV := "external_id,amount,timestamp,direction,business_id\n" +
		"P1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	req = httptest.NewRequest("POST", "/batches?source_id=processor&format=csv&row_policy=isolate&idempotency_key=k2", strings.NewReader(procCSV))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("submit processor: %d %s", rec.Code, rec.Body.String())
	}
	var procBatch model.Batch
	json.Unmarshal(rec.Body.Bytes(), &procBatch)

	// Start run.
	runBody, _ := json.Marshal(runRequest{RunID: "run1", BatchIDs: []string{internalBatch.ID, procBatch.ID}, Workers: 2})
	req = httptest.NewRequest("POST", "/runs", bytes.NewReader(runBody))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("start run: %d %s", rec.Code, rec.Body.String())
	}
	var run model.Run
	json.Unmarshal(rec.Body.Bytes(), &run)
	if run.Status != model.RunSucceeded {
		t.Errorf("run status = %s, want succeeded", run.Status)
	}

	// Get report.
	req = httptest.NewRequest("GET", "/runs/run1/report?format=json", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("report: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "match_groups") {
		t.Errorf("report body unexpected: %s", rec.Body.String())
	}

	// Get run.
	req = httptest.NewRequest("GET", "/runs/run1", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("get run: %d", rec.Code)
	}
}

func TestAPIIdempotency(t *testing.T) {
	svc := newTestService(t)
	srv := New(svc)
	csv := "external_id,amount,timestamp,direction\nI1,1.00,2026-01-02T03:04:05Z,credit\n"
	// Same idempotency key twice.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("POST", "/batches?source_id=internal&format=csv&row_policy=isolate&idempotency_key=samekey", strings.NewReader(csv))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != 201 {
			t.Fatalf("submit %d: %d %s", i, rec.Code, rec.Body.String())
		}
	}
	// Only one batch should exist.
	if bs := svc.ListBatches(); len(bs) != 1 {
		t.Errorf("batches = %d, want 1", len(bs))
	}
}

func TestAPIListBatchesAndRuns(t *testing.T) {
	svc := newTestService(t)
	srv := New(svc)
	req := httptest.NewRequest("GET", "/batches", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("list batches: %d", rec.Code)
	}
	req = httptest.NewRequest("GET", "/runs", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("list runs: %d", rec.Code)
	}
}
