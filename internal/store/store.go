// Package store is the persistence layer for SettleMesh. It is a pure-Go,
// transactional, in-process store with a schema that mirrors the bundled SQL
// migrations. The interface is designed so a SQLite (or other SQL) adapter can
// be slotted in behind the same methods; for tests and the default deployment
// the in-memory implementation is used so the test suite runs with zero
// external dependencies and no network.
//
// The store enforces:
//   - Idempotent batch submission via a unique idempotency key.
//   - The batch state machine (receiving -> validating -> committed/failed).
//   - Atomic batch publication (a staging set commits in a single transaction;
//     row-level failures either reject the whole batch or isolate the bad
//     rows).
//   - Immutable committed batches and completed reconciliation reports.
//   - Optimistic concurrency on run state transitions so cancel/commit races
//     resolve to exactly one terminal state.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
)

// ErrIdempotentConflict is returned when an idempotency key is reused with a
// different request body.
var ErrIdempotentConflict = errors.New("store: idempotency key reused with different request")

// ErrNotFound is returned when a referenced entity does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrInvalidTransition is returned when a state machine transition is illegal.
var ErrInvalidTransition = errors.New("store: invalid state transition")

// Store is the persistence interface.
type Store interface {
	// SubmitBatch stages a batch keyed by idempotencyKey. If the key already
	// exists and the file digest matches, the existing batch is returned (idempotent
	// retransmit). If the key exists with a different digest, ErrIdempotentConflict
	// is returned. If the key is new, a new batch is created. Different keys with
	// the same file digest create independent batches but a duplicate relation is
	// recorded between them.
	SubmitBatch(req BatchRequest) (*model.Batch, error)

	// CommitBatch publishes a staged batch atomically: valid records become
	// committed, invalid records are isolated (isolate policy) or cause a
	// failure (atomic policy). Returns the committed batch.
	CommitBatch(batchID string, records []*model.Record, rowErrors []*model.RowError, policy model.RowErrorPolicy) (*model.Batch, error)

	// GetBatch returns a batch by ID.
	GetBatch(batchID string) (*model.Batch, error)
	// ListBatches returns all batches in creation order.
	ListBatches() []*model.Batch
	// ListRecords returns all committed records for the given batches.
	ListRecords(batchIDs []string) []*model.Record

	// Rule revision management.
	PutRuleSet(rs *model.RuleSet) error
	GetRuleSet(revision int64) (*model.RuleSet, error)
	LatestRuleSet() (*model.RuleSet, error)

	// Reconciliation runs.
	CreateRun(run *model.Run) error
	UpdateRunStatus(runID string, status model.RunStatus, fn func(*model.Run) error) error
	// CommitRunResults atomically transitions a run to a terminal status and
	// persists its match groups, discrepancies and summary. It is the single
	// commit stage; the fn-based UpdateRunStatus must NOT call back into
	// locked store methods (that would deadlock), so run completion uses this
	// method instead.
	CommitRunResults(runID string, status model.RunStatus, groups []*model.MatchGroup, discreps []*model.Discrepancy, summary *model.ReportSummary) error
	GetRun(runID string) (*model.Run, error)
	ListRuns() []*model.Run

	// Duplicate relations scoped to a batch pair (for retransmit detection).
	RecordDuplicates(rels []*model.DuplicateRelation) error
	ListDuplicates() []*model.DuplicateRelation

	// SaveReport freezes a completed run's match groups, discrepancies and
	// report summary. Completed reports are immutable.
	SaveReport(runID string, groups []*model.MatchGroup, discreps []*model.Discrepancy, summary *model.ReportSummary) error
	GetReport(runID string) (groups []*model.MatchGroup, discreps []*model.Discrepancy, summary *model.ReportSummary, err error)

	// Recover transitions any non-terminal batches/runs to a safe state after
	// a restart. Unfinished batches go to failed; runs in running go to failed
	// unless their report is already saved (then succeeded).
	Recover() error
}

// BatchRequest is a batch submission request.
type BatchRequest struct {
	SourceID       string
	IdempotencyKey string
	FileDigest     string
	RowPolicy      model.RowErrorPolicy
}

// memStore is the default in-memory implementation.
type memStore struct {
	mu           sync.Mutex
	batches      map[string]*model.Batch
	byIdem       map[string]string          // idempotency key -> batch ID
	byDigest     map[string][]string        // file digest -> batch IDs (for retransmit)
	records      map[string][]*model.Record // batch ID -> records
	ruleSets     map[int64]*model.RuleSet
	latestRule   int64
	runs         map[string]*model.Run
	dups         []*model.DuplicateRelation
	reportGroups map[string][]*model.MatchGroup
	reportDiscs  map[string][]*model.Discrepancy
	reportSumm   map[string]*model.ReportSummary
	clock        func() time.Time
	seq          uint64 // monotonic counter for unique IDs (fixed-clock safe)
}

// New returns an in-memory store. clock may be nil (uses time.Now).
func New(clock func() time.Time) Store {
	if clock == nil {
		clock = time.Now
	}
	return &memStore{
		batches:      make(map[string]*model.Batch),
		byIdem:       make(map[string]string),
		byDigest:     make(map[string][]string),
		records:      make(map[string][]*model.Record),
		ruleSets:     make(map[int64]*model.RuleSet),
		runs:         make(map[string]*model.Run),
		reportGroups: make(map[string][]*model.MatchGroup),
		reportDiscs:  make(map[string][]*model.Discrepancy),
		reportSumm:   make(map[string]*model.ReportSummary),
		clock:        clock,
	}
}

func (s *memStore) SubmitBatch(req BatchRequest) (*model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Idempotency.
	if bid, ok := s.byIdem[req.IdempotencyKey]; ok {
		existing := s.batches[bid]
		if existing.FileDigest != req.FileDigest {
			return nil, ErrIdempotentConflict
		}
		return existing, nil
	}
	batchID := s.newID("batch")
	b := &model.Batch{
		ID:             batchID,
		SourceID:       req.SourceID,
		Status:         model.BatchReceiving,
		IdempotencyKey: req.IdempotencyKey,
		FileDigest:     req.FileDigest,
		RowPolicy:      req.RowPolicy,
		CreatedAt:      s.clock(),
		Revision:       1,
	}
	s.batches[batchID] = b
	s.byIdem[req.IdempotencyKey] = batchID
	// Track digest for retransmit detection across different idempotency keys.
	digests := s.byDigest[req.FileDigest]
	for _, otherID := range digests {
		// Retransmit relation: this batch re-uploads the same file. We record it
		// lazily at commit time when records are available; here we just track.
		_ = otherID
	}
	s.byDigest[req.FileDigest] = append(s.byDigest[req.FileDigest], batchID)
	return b, nil
}

func (s *memStore) CommitBatch(batchID string, records []*model.Record, rowErrors []*model.RowError, policy model.RowErrorPolicy) (*model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok {
		return nil, ErrNotFound
	}
	// Idempotent re-commit: a concurrent submit with the same idempotency key
	// may have already committed this batch. Return it as-is rather than
	// erroring. This is what makes concurrent same-key submits resolve to a
	// single committed batch under the store mutex.
	if b.Status == model.BatchCommitted || b.Status == model.BatchFailed {
		return b, nil
	}
	if b.Status != model.BatchReceiving && b.Status != model.BatchValidating {
		return nil, ErrInvalidTransition
	}
	// Apply row error policy.
	var valid []*model.Record
	var isolated []*model.Record
	for _, r := range records {
		if r.Isolated {
			isolated = append(isolated, r)
		} else {
			valid = append(valid, r)
		}
	}
	if policy == model.RowPolicyAtomic && len(isolated) > 0 {
		// Reject the whole batch.
		b.Status = model.BatchFailed
		now := s.clock()
		b.CommittedAt = &now
		b.Revision++
		return b, nil
	}
	// Isolate mode: publish valid records, keep isolated ones.
	for _, r := range valid {
		r.BatchID = batchID
	}
	for _, r := range isolated {
		r.BatchID = batchID
	}
	all := append(append([]*model.Record(nil), valid...), isolated...)
	s.records[batchID] = all

	b.Status = model.BatchCommitted
	now := s.clock()
	b.CommittedAt = &now
	b.Revision++
	b.Summary = computeSummary(batchID, b.SourceID, all, rowErrors)
	// Detect retransmit duplicates against prior batches with the same digest.
	s.recordRetransmit(batchID, valid)
	return b, nil
}

func (s *memStore) recordRetransmit(batchID string, records []*model.Record) {
	b := s.batches[batchID]
	for _, otherID := range s.byDigest[b.FileDigest] {
		if otherID == batchID {
			continue
		}
		// For each record in this batch, find a content+external-id match in
		// the other batch and record a retransmit relation.
		other := s.records[otherID]
		byKey := make(map[string]*model.Record)
		for _, r := range other {
			byKey[r.SourceID+"|"+r.ExternalID+"|"+r.ContentHash] = r
		}
		for _, r := range records {
			key := r.SourceID + "|" + r.ExternalID + "|" + r.ContentHash
			if canon, ok := byKey[key]; ok {
				rel := &model.DuplicateRelation{
					ID:                dupID(model.DupRetransmit, canon.ID, r.ID),
					Type:              model.DupRetransmit,
					CanonicalRecordID: canon.ID,
					DuplicateRecordID: r.ID,
					SourceID:          r.SourceID,
					Action:            model.DupReport,
				}
				s.dups = append(s.dups, rel)
			}
		}
	}
}

func computeSummary(batchID, sourceID string, records []*model.Record, rowErrors []*model.RowError) *model.BatchSummary {
	var validCount, invalidCount int
	var sum int64
	var digestParts []string
	for _, r := range records {
		if r.Isolated {
			invalidCount++
		} else {
			validCount++
			sum += r.Amount.V
			digestParts = append(digestParts, r.ID)
		}
	}
	sort.Strings(digestParts)
	h := sha256.New()
	for _, p := range digestParts {
		fmt.Fprintln(h, p)
	}
	curCode := ""
	if len(records) > 0 {
		curCode = records[0].Amount.C.Code
	}
	return &model.BatchSummary{
		BatchID:         batchID,
		SourceID:        sourceID,
		RecordCount:     len(records),
		ValidCount:      validCount,
		InvalidCount:    invalidCount,
		AmountSumMinor:  sum,
		AmountSumString: fmt.Sprintf("%d %s", sum, curCode),
		ContentDigest:   hex.EncodeToString(h.Sum(nil))[:16],
	}
}

func (s *memStore) GetBatch(batchID string) (*model.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[batchID]
	if !ok {
		return nil, ErrNotFound
	}
	return b, nil
}

func (s *memStore) ListBatches() []*model.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Batch, 0, len(s.batches))
	for _, b := range s.batches {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *memStore) ListRecords(batchIDs []string) []*model.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.Record
	for _, bid := range batchIDs {
		out = append(out, s.records[bid]...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].SourceLine < out[j].SourceLine
	})
	return out
}

func (s *memStore) PutRuleSet(rs *model.RuleSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ruleSets[rs.Revision] = rs
	if rs.Revision > s.latestRule {
		s.latestRule = rs.Revision
	}
	return nil
}

func (s *memStore) GetRuleSet(revision int64) (*model.RuleSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs, ok := s.ruleSets[revision]
	if !ok {
		return nil, ErrNotFound
	}
	return rs, nil
}

func (s *memStore) LatestRuleSet() (*model.RuleSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latestRule == 0 {
		return nil, ErrNotFound
	}
	return s.ruleSets[s.latestRule], nil
}

func (s *memStore) CreateRun(run *model.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[run.ID]; ok {
		return ErrInvalidTransition
	}
	s.runs[run.ID] = run
	return nil
}

// UpdateRunStatus performs an optimistic state transition: fn may mutate the
// run in-memory (e.g. attach a fault point) but must NOT call back into locked
// store methods. The transition is committed only if the run is not already
// terminal. This makes cancel/commit races resolve to exactly one terminal
// state. For transitions that must also persist a report atomically, use
// CommitRunResults instead (it never re-enters the lock).
func (s *memStore) UpdateRunStatus(runID string, status model.RunStatus, fn func(*model.Run) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return ErrNotFound
	}
	if run.Status == model.RunSucceeded || run.Status == model.RunFailed || run.Status == model.RunCancelled {
		return ErrInvalidTransition
	}
	if fn != nil {
		if err := fn(run); err != nil {
			return err
		}
	}
	run.Status = status
	now := s.clock()
	run.FinishedAt = &now
	return nil
}

// CommitRunResults atomically transitions a run to a terminal status and
// persists its match groups, discrepancies and summary. It is the single
// commit stage for a reconciliation run. If the run is already terminal,
// ErrInvalidTransition is returned and nothing is persisted.
func (s *memStore) CommitRunResults(runID string, status model.RunStatus, groups []*model.MatchGroup, discreps []*model.Discrepancy, summary *model.ReportSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return ErrNotFound
	}
	if run.Status == model.RunSucceeded || run.Status == model.RunFailed || run.Status == model.RunCancelled {
		return ErrInvalidTransition
	}
	if _, exists := s.reportSumm[runID]; exists {
		return ErrInvalidTransition
	}
	s.reportGroups[runID] = copyGroups(groups)
	s.reportDiscs[runID] = copyDiscs(discreps)
	s.reportSumm[runID] = summary
	run.MatchGroups = groups
	run.Discrepancies = discreps
	run.Summary = summary
	run.Status = status
	now := s.clock()
	run.FinishedAt = &now
	return nil
}

func (s *memStore) GetRun(runID string) (*model.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return nil, ErrNotFound
	}
	return run, nil
}

func (s *memStore) ListRuns() []*model.Run {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Run, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, r)
	}
	return out
}

func (s *memStore) RecordDuplicates(rels []*model.DuplicateRelation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dups = append(s.dups, rels...)
	return nil
}

func (s *memStore) ListDuplicates() []*model.DuplicateRelation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]*model.DuplicateRelation(nil), s.dups...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *memStore) SaveReport(runID string, groups []*model.MatchGroup, discreps []*model.Discrepancy, summary *model.ReportSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.reportSumm[runID]; ok {
		return ErrInvalidTransition // immutable
	}
	// Deep copy to ensure immutability.
	s.reportGroups[runID] = copyGroups(groups)
	s.reportDiscs[runID] = copyDiscs(discreps)
	s.reportSumm[runID] = summary
	return nil
}

func (s *memStore) GetReport(runID string) ([]*model.MatchGroup, []*model.Discrepancy, *model.ReportSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	summ, ok := s.reportSumm[runID]
	if !ok {
		return nil, nil, nil, ErrNotFound
	}
	return s.reportGroups[runID], s.reportDiscs[runID], summ, nil
}

func (s *memStore) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.batches {
		if b.Status == model.BatchReceiving || b.Status == model.BatchValidating {
			// Unfinished staging: fail the batch. Any staging records are
			// discarded (they were never committed).
			b.Status = model.BatchFailed
			b.Revision++
			delete(s.records, b.ID)
		}
	}
	for _, r := range s.runs {
		if r.Status == model.RunPending || r.Status == model.RunRunning {
			if _, ok := s.reportSumm[r.ID]; ok {
				r.Status = model.RunSucceeded
			} else {
				r.Status = model.RunFailed
				r.FaultPoint = "recovery: incomplete run after restart"
			}
			now := s.clock()
			r.FinishedAt = &now
		}
	}
	return nil
}

func copyGroups(groups []*model.MatchGroup) []*model.MatchGroup {
	out := make([]*model.MatchGroup, len(groups))
	for i, g := range groups {
		ng := *g
		ng.RecordIDs = append([]string(nil), g.RecordIDs...)
		ng.ScoreTuple = append([]int64(nil), g.ScoreTuple...)
		out[i] = &ng
	}
	return out
}
func copyDiscs(ds []*model.Discrepancy) []*model.Discrepancy {
	out := make([]*model.Discrepancy, len(ds))
	for i, d := range ds {
		nd := *d
		nd.Evidence = append([]model.Evidence(nil), d.Evidence...)
		out[i] = &nd
	}
	return out
}

func (s *memStore) newID(prefix string) string {
	s.seq++
	// The monotonic seq guarantees uniqueness even under a fixed test clock;
	// the clock contributes human-readable ordering. Business-identity IDs
	// (match groups, discrepancies, duplicate relations) are content-derived
	// and thus deterministic; batch IDs are not part of the deterministic
	// business output.
	return fmt.Sprintf("%s_%016x", prefix, s.seq)
}

func dupID(dt model.DupType, a, b string) string {
	if a > b {
		a, b = b, a
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s", dt, a, b)
	return "dup_" + hex.EncodeToString(h.Sum(nil))[:16]
}
