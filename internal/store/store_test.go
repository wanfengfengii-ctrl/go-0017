package store

import (
	"sync"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

var fixedClock = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func mkRec(role model.Role, ext, batch string, amount int64) *model.Record {
	return &model.Record{
		ID:          "rec_" + string(role) + "_" + ext + "_" + batch,
		SourceID:    string(role),
		Role:        role,
		ExternalID:  ext,
		BatchID:     batch,
		Amount:      money.FromMinor(money.USD, amount),
		StableKey:   string(role) + "|" + ext + "|" + batch,
		Timestamp:   fixedClock(),
		ContentHash: ext + batch,
	}
}

func TestIdempotentSubmit(t *testing.T) {
	s := New(fixedClock)
	req := BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "d1", RowPolicy: model.RowPolicyIsolate}
	b1, err := s.SubmitBatch(req)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := s.SubmitBatch(req)
	if err != nil {
		t.Fatal(err)
	}
	if b1.ID != b2.ID {
		t.Errorf("same idempotency key should return same batch: %s vs %s", b1.ID, b2.ID)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	s := New(fixedClock)
	_, err := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "DIFFERENT"})
	if err != ErrIdempotentConflict {
		t.Errorf("err = %v, want ErrIdempotentConflict", err)
	}
}

func TestRetransmitDifferentKey(t *testing.T) {
	s := New(fixedClock)
	// Same file digest, different idempotency keys -> independent batches.
	b1, err := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "D"})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k2", FileDigest: "D"})
	if err != nil {
		t.Fatal(err)
	}
	if b1.ID == b2.ID {
		t.Fatal("different keys should create different batches")
	}
	// Commit both with the same content; a retransmit duplicate relation should
	// be recorded.
	recs1 := []*model.Record{mkRec(model.RoleProcessor, "TX1", b1.ID, 1000)}
	if _, err := s.CommitBatch(b1.ID, recs1, nil, model.RowPolicyIsolate); err != nil {
		t.Fatal(err)
	}
	recs2 := []*model.Record{mkRec(model.RoleProcessor, "TX1", b2.ID, 1000)}
	// Content hash must match for retransmit detection: use same hash.
	recs2[0].ContentHash = recs1[0].ContentHash
	if _, err := s.CommitBatch(b2.ID, recs2, nil, model.RowPolicyIsolate); err != nil {
		t.Fatal(err)
	}
	dups := s.ListDuplicates()
	found := false
	for _, d := range dups {
		if d.Type == model.DupRetransmit {
			found = true
		}
	}
	if !found {
		t.Error("expected retransmit duplicate relation across different keys")
	}
}

func TestAtomicReject(t *testing.T) {
	s := New(fixedClock)
	b, err := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "d1", RowPolicy: model.RowPolicyAtomic})
	if err != nil {
		t.Fatal(err)
	}
	recs := []*model.Record{
		mkRec(model.RoleProcessor, "OK", b.ID, 1000),
		{ID: "bad", SourceID: "processor", Role: model.RoleProcessor, ExternalID: "BAD", BatchID: b.ID, Isolated: true, Amount: money.FromMinor(money.USD, 0), Timestamp: fixedClock()},
	}
	b2, err := s.CommitBatch(b.ID, recs, nil, model.RowPolicyAtomic)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Status != model.BatchFailed {
		t.Errorf("status = %s, want failed (atomic)", b2.Status)
	}
	// No records should be queryable.
	if recs := s.ListRecords([]string{b.ID}); len(recs) != 0 {
		t.Errorf("atomic reject should publish 0 records, got %d", len(recs))
	}
}

func TestIsolateMode(t *testing.T) {
	s := New(fixedClock)
	b, err := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "d1", RowPolicy: model.RowPolicyIsolate})
	if err != nil {
		t.Fatal(err)
	}
	recs := []*model.Record{
		mkRec(model.RoleProcessor, "OK", b.ID, 1000),
		{ID: "bad", SourceID: "processor", Role: model.RoleProcessor, ExternalID: "BAD", BatchID: b.ID, Isolated: true, InvalidReason: "bad", Amount: money.FromMinor(money.USD, 0), Timestamp: fixedClock()},
	}
	b2, err := s.CommitBatch(b.ID, recs, nil, model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	if b2.Status != model.BatchCommitted {
		t.Fatalf("status = %s, want committed (isolate)", b2.Status)
	}
	if b2.Summary.ValidCount != 1 || b2.Summary.InvalidCount != 1 {
		t.Errorf("counts: valid=%d invalid=%d", b2.Summary.ValidCount, b2.Summary.InvalidCount)
	}
}

func TestRunTerminalImmutability(t *testing.T) {
	s := New(fixedClock)
	run := &model.Run{ID: "run1", Status: model.RunRunning}
	if err := s.CreateRun(run); err != nil {
		t.Fatal(err)
	}
	// Cancel it.
	if err := s.UpdateRunStatus("run1", model.RunCancelled, nil); err != nil {
		t.Fatal(err)
	}
	// Subsequent commit attempt should fail.
	err := s.UpdateRunStatus("run1", model.RunSucceeded, func(r *model.Run) error {
		r.MatchGroups = []*model.MatchGroup{{ID: "mg"}}
		return nil
	})
	if err != ErrInvalidTransition {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
}

func TestConcurrentCancelCommit(t *testing.T) {
	// Only one terminal state wins.
	s := New(fixedClock)
	run := &model.Run{ID: "run1", Status: model.RunRunning}
	if err := s.CreateRun(run); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var success, cancel int
	var mu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			if i%2 == 0 {
				err = s.UpdateRunStatus("run1", model.RunSucceeded, nil)
			} else {
				err = s.UpdateRunStatus("run1", model.RunCancelled, nil)
			}
			mu.Lock()
			if err == nil {
				if i%2 == 0 {
					success++
				} else {
					cancel++
				}
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if success != 1 && cancel != 1 {
		if success+cancel != 1 {
			t.Errorf("expected exactly one terminal transition, got success=%d cancel=%d", success, cancel)
		}
	}
	r, _ := s.GetRun("run1")
	if r.Status != model.RunSucceeded && r.Status != model.RunCancelled {
		t.Errorf("final status = %s", r.Status)
	}
}

func TestReportImmutability(t *testing.T) {
	s := New(fixedClock)
	if err := s.SaveReport("run1", []*model.MatchGroup{{ID: "mg"}}, []*model.Discrepancy{{ID: "d"}}, &model.ReportSummary{RunID: "run1"}); err != nil {
		t.Fatal(err)
	}
	err := s.SaveReport("run1", nil, nil, nil)
	if err != ErrInvalidTransition {
		t.Errorf("err = %v, want ErrInvalidTransition", err)
	}
	g, d, summ, err := s.GetReport("run1")
	if err != nil {
		t.Fatal(err)
	}
	if len(g) != 1 || len(d) != 1 || summ == nil {
		t.Errorf("report retrieval wrong: g=%d d=%d summ=%v", len(g), len(d), summ)
	}
}

func TestRecover(t *testing.T) {
	s := New(fixedClock)
	// Unfinished batch.
	b, _ := s.SubmitBatch(BatchRequest{SourceID: "processor", IdempotencyKey: "k1", FileDigest: "d1"})
	// staging records (never committed) — simulate by direct insertion.
	s.(*memStore).records[b.ID] = []*model.Record{mkRec(model.RoleProcessor, "X", b.ID, 1)}
	// Unfinished run.
	s.CreateRun(&model.Run{ID: "run1", Status: model.RunRunning})
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	b2, _ := s.GetBatch(b.ID)
	if b2.Status != model.BatchFailed {
		t.Errorf("batch status = %s, want failed", b2.Status)
	}
	if recs := s.ListRecords([]string{b.ID}); len(recs) != 0 {
		t.Errorf("staging records should be cleared, got %d", len(recs))
	}
	r, _ := s.GetRun("run1")
	if r.Status != model.RunFailed {
		t.Errorf("run status = %s, want failed", r.Status)
	}
}

func TestRuleSet(t *testing.T) {
	s := New(fixedClock)
	rs1 := &model.RuleSet{Revision: 1, Rules: []*model.Rule{{ID: "r1"}}}
	rs2 := &model.RuleSet{Revision: 2, Rules: []*model.Rule{{ID: "r2"}}}
	s.PutRuleSet(rs1)
	s.PutRuleSet(rs2)
	latest, err := s.LatestRuleSet()
	if err != nil {
		t.Fatal(err)
	}
	if latest.Revision != 2 {
		t.Errorf("latest = %d, want 2", latest.Revision)
	}
	got, err := s.GetRuleSet(1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 1 {
		t.Errorf("rev 1 = %d", got.Revision)
	}
}
