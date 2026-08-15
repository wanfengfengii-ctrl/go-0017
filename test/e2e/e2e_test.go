// Package e2e holds end-to-end acceptance tests that drive the full pipeline
// (ingest -> dedup -> matching -> reconcile -> report) over the fixed sample
// feeds in /samples. These tests assert the determinism and precision
// guarantees of the system: identical output for any worker count and any
// controlled completion order, and a stable report content digest.
package e2e

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/reconcile"
	"github.com/settlemesh/settlemesh/internal/report"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/testcontrol"
)

func TestE2EDeterminismAcrossWorkers(t *testing.T) {
	fix := ingestSamples(t)
	snap := &reconcile.Snapshot{
		RunID:    "run1",
		Records:  fix.records,
		RuleSet:  fix.ruleSet,
		BatchIDs: []string{"b-internal", "b-processor", "b-bank"},
	}
	coord := reconcile.NewCoordinator(fixedTime())
	baseline := coord.Run(snap, 1)
	baseRep := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, baseline.MatchGroups, baseline.Discrepancies, fix.byID)
	baseDigest := baseRep.ContentDigest()
	baseJSON, _ := baseRep.JSON()

	for _, w := range []int{1, 2, 8} {
		r := coord.Run(snap, w)
		rep := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, r.MatchGroups, r.Discrepancies, fix.byID)
		if rep.ContentDigest() != baseDigest {
			t.Errorf("workers=%d: content digest differs", w)
		}
		j, _ := rep.JSON()
		if !bytes.Equal(j, baseJSON) {
			t.Errorf("workers=%d: report JSON differs from baseline", w)
		}
		if !sameIDs(groupIDs(baseline.MatchGroups), groupIDs(r.MatchGroups)) {
			t.Errorf("workers=%d: match group IDs differ", w)
		}
		if !sameIDs(discIDs(baseline.Discrepancies), discIDs(r.Discrepancies)) {
			t.Errorf("workers=%d: discrepancy IDs differ", w)
		}
		if !sameIDs(dupIDs(baseline.Duplicates), dupIDs(r.Duplicates)) {
			t.Errorf("workers=%d: duplicate IDs differ", w)
		}
	}
}

func TestE2EExpectedCounts(t *testing.T) {
	fix := ingestSamples(t)
	snap := &reconcile.Snapshot{RunID: "run1", Records: fix.records, RuleSet: fix.ruleSet, BatchIDs: []string{"b-internal", "b-processor", "b-bank"}}
	coord := reconcile.NewCoordinator(fixedTime())
	r := coord.Run(snap, 4)

	counts := map[model.DiscType]int{}
	for _, d := range r.Discrepancies {
		counts[d.Type]++
	}
	// BIZ100/050/075/200 are exact-id triples across internal+processor+bank.
	// INT005 (BIZ030) matches processor PRC005 (exact-id pair). INT006 has two
	// processor candidates (PRC006a/PRC006b) tying -> ambiguous.
	if len(r.MatchGroups) < 5 {
		t.Errorf("expected >=5 match groups, got %d", len(r.MatchGroups))
	}
	if counts[model.DiscAmbiguous] < 1 {
		t.Errorf("expected >=1 ambiguous, got %d (discs: %v)", counts[model.DiscAmbiguous], discTypeList(r.Discrepancies))
	}
	t.Logf("groups=%d discrepancies=%v", len(r.MatchGroups), counts)
}

func TestE2EAtomicRejectVsIsolate(t *testing.T) {
	bad := "external_id,amount,timestamp,direction,business_id\n" +
		"GOOD,10.00,2026-01-02T03:04:05Z,credit,B1\n" +
		"BAD,notanumber,2026-01-02T03:04:05Z,credit,B2\n"
	atomic := ingestString(t, model.RoleInternal, bad, model.RowPolicyAtomic)
	isolate := ingestString(t, model.RoleInternal, bad, model.RowPolicyIsolate)
	if len(atomic) != 0 {
		t.Errorf("atomic should publish 0 valid records, got %d", len(atomic))
	}
	if len(isolate) == 0 {
		t.Errorf("isolate should publish >=1 valid record")
	}
}

func TestE2EIdempotentConcurrentSubmit(t *testing.T) {
	svc := newService(t)
	csv := readSample(t, "internal.csv")
	var wg sync.WaitGroup
	var mu sync.Mutex
	ids := map[string]bool{}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := svc.SubmitAndCommit("internal", "idem-key-1", "csv", strings.NewReader(csv), model.RowPolicyIsolate)
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			mu.Lock()
			ids[b.ID] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(ids) != 1 {
		t.Errorf("expected 1 batch from concurrent same-key submit, got %d: %v", len(ids), ids)
	}
}

func TestE2ESnapshotIsolation(t *testing.T) {
	svc := newService(t)
	submitSample(t, svc, "internal", "internal.csv", "k-i1")
	submitSample(t, svc, "processor", "processor.csv", "k-p1")
	ids := committedBatchIDs(svc)
	run1, err := svc.StartRun("run1", ids, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if run1.Status != model.RunSucceeded {
		t.Fatalf("run1 status = %s", run1.Status)
	}
	// New batch arrives after run1 started.
	bankID := submitSample(t, svc, "bank", "bank.csv", "k-b1")
	if contains(run1.Snapshot.BatchIDs, bankID) {
		t.Error("run1 snapshot leaked the later batch")
	}
	ids2 := append([]string(nil), ids...)
	ids2 = append(ids2, bankID)
	run2, err := svc.StartRun("run2", ids2, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(run1.Snapshot.BatchIDs) == len(run2.Snapshot.BatchIDs) {
		t.Error("run1 and run2 snapshots should differ in size")
	}
}

func TestE2EFaultInjection(t *testing.T) {
	ctrl := newControlledService(t)
	ctrl.submitSample(model.RoleInternal, "internal.csv", "k-i")
	ctrl.submitSample(model.RoleProcessor, "processor.csv", "k-p")
	ctrl.control.SetFault(reconcileFault(), reconcileFaultErr())
	_, err := ctrl.svc.StartRun("run1", ctrl.batchIDs(), 1, 2)
	if err == nil {
		t.Fatal("expected fault error")
	}
	run, _ := ctrl.svc.GetRun("run1")
	if run.Status != model.RunFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
	if _, _, _, err := ctrl.store.GetReport("run1"); err == nil {
		t.Error("no half-report should be persisted on faulted run")
	}
	// Clear fault and retry with a fresh run id (the faulted run is terminal).
	ctrl.control.ClearFault(reconcileFault())
	if _, err := ctrl.svc.StartRun("run1b", ctrl.batchIDs(), 1, 2); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestE2EFaultAtCandidateGen(t *testing.T) {
	ctrl := newControlledService(t)
	ctrl.submitSample(model.RoleInternal, "internal.csv", "k-i")
	ctrl.submitSample(model.RoleProcessor, "processor.csv", "k-p")
	ctrl.control.SetFault(testcontrol.FaultCandidateGen, reconcileFaultErr())
	_, err := ctrl.svc.StartRun("run1", ctrl.batchIDs(), 1, 2)
	if err == nil {
		t.Fatal("expected candidate_gen fault")
	}
	run, _ := ctrl.svc.GetRun("run1")
	if run.Status != model.RunFailed {
		t.Errorf("status = %s, want failed", run.Status)
	}
}

func TestE2EReportSorting(t *testing.T) {
	fix := ingestSamples(t)
	snap := &reconcile.Snapshot{RunID: "run1", Records: fix.records, RuleSet: fix.ruleSet, BatchIDs: []string{"b-i", "b-p", "b-b"}}
	r := reconcile.NewCoordinator(fixedTime()).Run(snap, 4)
	rep := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, r.MatchGroups, r.Discrepancies, fix.byID)
	for i := 1; i < len(rep.Discrepancies); i++ {
		a, b := rep.Discrepancies[i-1], rep.Discrepancies[i]
		if a.Currency > b.Currency {
			t.Errorf("discrepancies not sorted by currency at %d", i)
		}
		if a.Currency == b.Currency && a.Type > b.Type {
			t.Errorf("discrepancies not sorted by type at %d", i)
		}
	}
}

func TestE2ENoFloatPrecision(t *testing.T) {
	fix := ingestSamples(t)
	found := false
	for _, r := range fix.records {
		if r.ExternalID == "INT006" {
			found = true
			if r.Amount.V != 8888 {
				t.Errorf("INT006 amount = %d, want 8888", r.Amount.V)
			}
		}
	}
	if !found {
		t.Error("INT006 not found in fixture")
	}
}

func TestE2EReportDeterminismByteIdentical(t *testing.T) {
	// Same snapshot rerun must produce byte-identical JSON and CSV.
	fix := ingestSamples(t)
	snap := &reconcile.Snapshot{RunID: "run1", Records: fix.records, RuleSet: fix.ruleSet, BatchIDs: []string{"b-i", "b-p", "b-b"}}
	coord := reconcile.NewCoordinator(fixedTime())
	r1 := coord.Run(snap, 3)
	r2 := coord.Run(snap, 5)
	rep1 := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, r1.MatchGroups, r1.Discrepancies, fix.byID)
	rep2 := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, r2.MatchGroups, r2.Discrepancies, fix.byID)
	j1, _ := rep1.JSON()
	j2, _ := rep2.JSON()
	if !bytes.Equal(j1, j2) {
		t.Errorf("report JSON not byte-identical across reruns")
	}
	c1, _ := rep1.CSV()
	c2, _ := rep2.CSV()
	if !bytes.Equal(c1, c2) {
		t.Errorf("report CSV not byte-identical across reruns")
	}
}

// --- helpers ---

func groupIDs(gs []*model.MatchGroup) []string {
	out := make([]string, len(gs))
	for i, g := range gs {
		out[i] = g.ID
	}
	return out
}
func discIDs(ds []*model.Discrepancy) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}
func dupIDs(ds []*model.DuplicateRelation) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}
func sameIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
		if m[x] < 0 {
			return false
		}
	}
	return true
}
func discTypeList(ds []*model.Discrepancy) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = string(d.Type)
	}
	return out
}
func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

func committedBatchIDs(s interface{ ListBatches() []*model.Batch }) []string {
	bs := s.ListBatches()
	ids := make([]string, 0, len(bs))
	for _, b := range bs {
		if b.Status == model.BatchCommitted {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

// candidateGenFaultPoint is retained for compatibility; tests use
// testcontrol.FaultCandidateGen directly.
func candidateGenFaultPoint() testcontrol.FaultPoint { return testcontrol.FaultCandidateGen }

var _ = time.Second
var _ service.Service
var _ = bytes.Equal
