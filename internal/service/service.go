// Package service is the application layer that wires the store, ingest
// pipeline, dedup/matching engine, reconciliation coordinator and report
// builder into a coherent workflow. It exposes the operations the HTTP API and
// CLI drive: creating sources and rule sets, submitting and committing import
// batches, starting reconciliation runs, and exporting reports.
package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/settlemesh/settlemesh/internal/ingest"
	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
	"github.com/settlemesh/settlemesh/internal/reconcile"
	"github.com/settlemesh/settlemesh/internal/report"
	"github.com/settlemesh/settlemesh/internal/store"
	"github.com/settlemesh/settlemesh/internal/testcontrol"
)

// Service holds configured sources and orchestrates operations.
type Service struct {
	mu       sync.Mutex
	sources  map[string]*model.Source
	store    store.Store
	registry *money.Registry
	clock    func() time.Time
	control  *testcontrol.Control
	workers  int
}

// Option configures a Service.
type Option func(*Service)

// WithClock sets a fixed clock (used in tests).
func WithClock(fn func() time.Time) Option {
	return func(s *Service) { s.clock = fn }
}

// WithControl wires a test control plane.
func WithControl(c *testcontrol.Control) Option {
	return func(s *Service) { s.control = c }
}

// WithWorkers sets the ingest worker count.
func WithWorkers(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.workers = n
		}
	}
}

// New constructs a Service over the given store.
func New(s store.Store, opts ...Option) *Service {
	svc := &Service{
		sources:  make(map[string]*model.Source),
		store:    s,
		registry: money.NewRegistry(),
		clock:    time.Now,
		workers:  4,
	}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

// AddSource registers a source.
func (s *Service) AddSource(src *model.Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if src.ID == "" {
		return fmt.Errorf("service: source id required")
	}
	s.sources[src.ID] = src
	return nil
}

// GetSource returns a registered source.
func (s *Service) GetSource(id string) (*model.Source, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.sources[id]
	if !ok {
		return nil, fmt.Errorf("service: source %q not found", id)
	}
	return src, nil
}

// PutRuleSet stores a rule set revision.
func (s *Service) PutRuleSet(rs *model.RuleSet) error {
	return s.store.PutRuleSet(rs)
}

// LatestRuleSet returns the latest rule set.
func (s *Service) LatestRuleSet() (*model.RuleSet, error) {
	return s.store.LatestRuleSet()
}

// SubmitAndCommit is the convenience that submits a batch, parses the feed,
// and commits in one call. It is the primary import path for the API/CLI.
func (s *Service) SubmitAndCommit(sourceID, idempotencyKey string, format string, r io.Reader, policy model.RowErrorPolicy) (*model.Batch, error) {
	src, err := s.GetSource(sourceID)
	if err != nil {
		return nil, err
	}
	if format != "" {
		// override format if provided
		srcCopy := *src
		srcCopy.Format = format
		src = &srcCopy
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("service: read feed: %w", err)
	}
	digest := fileDigest(data)
	b, err := s.store.SubmitBatch(store.BatchRequest{
		SourceID:       sourceID,
		IdempotencyKey: idempotencyKey,
		FileDigest:     digest,
		RowPolicy:      policy,
	})
	if err != nil {
		return nil, err
	}
	// Idempotent retransmit: if already committed, return without re-parsing.
	if b.Status == model.BatchCommitted || b.Status == model.BatchFailed {
		return b, nil
	}
	// Parse the feed.
	if fault := s.fault(testcontrol.FaultParse); fault != nil {
		s.failBatch(b.ID)
		return nil, fault
	}
	p := ingest.New(ingest.Options{Source: src, Workers: s.workers, Registry: s.registry})
	var records []*model.Record
	var rowErrors []*model.RowError
	if err := p.Run(bytes.NewReader(data), func(res ingest.RowResult) error {
		if res.Record != nil {
			records = append(records, res.Record)
		} else if res.Error != nil {
			re := model.RowError{Line: res.Error.Line, Field: res.Error.Field, Code: res.Error.Code, Reason: res.Error.Reason, Value: res.Error.Value}
			// Isolated record keeps a minimal representation for evidence.
			records = append(records, &model.Record{
				ID:            fmt.Sprintf("rec_invalid_%s_%d", sourceID, res.Line),
				SourceID:      sourceID,
				Role:          src.Role,
				ExternalID:    "",
				SourceLine:    res.Line,
				BatchID:       b.ID,
				Isolated:      true,
				InvalidReason: re.Reason,
				Amount:        money.Zero(s.currency(src)),
				Timestamp:     s.clock(),
				StableKey:     fmt.Sprintf("%s|invalid|%d", src.Role, res.Line),
			})
			rowErrors = append(rowErrors, &re)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("service: ingest: %w", err)
	}

	if fault := s.fault(testcontrol.FaultStagingCommit); fault != nil {
		s.failBatch(b.ID)
		return nil, fault
	}
	committed, err := s.store.CommitBatch(b.ID, records, rowErrors, policy)
	if err != nil {
		return nil, err
	}
	return committed, nil
}

func (s *Service) failBatch(id string) {
	_, _ = s.store.CommitBatch(id, nil, nil, model.RowPolicyAtomic)
}

func (s *Service) currency(src *model.Source) money.Currency {
	if c, ok := s.registry.Lookup(src.Currency); ok {
		return c
	}
	return money.USD
}

// StartRun freezes a snapshot from the given committed batch IDs and the latest
// (or specified) rule revision, runs reconciliation, and persists the report.
func (s *Service) StartRun(runID string, batchIDs []string, ruleRevision int64, workers int) (*model.Run, error) {
	if workers < 1 {
		workers = 1
	}
	// Build snapshot.
	records := s.store.ListRecords(batchIDs)
	var summaries []model.BatchSummary
	for _, bid := range batchIDs {
		b, err := s.store.GetBatch(bid)
		if err != nil {
			return nil, err
		}
		if b.Status != model.BatchCommitted {
			return nil, fmt.Errorf("service: batch %s not committed (status %s)", bid, b.Status)
		}
		if b.Summary != nil {
			summaries = append(summaries, *b.Summary)
		}
	}
	var rs *model.RuleSet
	var err error
	if ruleRevision > 0 {
		rs, err = s.store.GetRuleSet(ruleRevision)
	} else {
		rs, err = s.store.LatestRuleSet()
	}
	if err != nil {
		return nil, err
	}
	snap := &reconcile.Snapshot{
		RunID:          runID,
		BatchIDs:       append([]string(nil), batchIDs...),
		Records:        records,
		RuleSet:        rs,
		BatchSummaries: summaries,
	}
	run := &model.Run{
		ID:     runID,
		Status: model.RunRunning,
		Snapshot: &model.RunSnapshot{
			RunID:          runID,
			BatchIDs:       append([]string(nil), batchIDs...),
			BatchSummaries: summaries,
			RuleRevision:   rs.Revision,
			CreatedAt:      s.clock(),
		},
	}
	if err := s.store.CreateRun(run); err != nil {
		return nil, err
	}

	// Candidate generation fault.
	if fault := s.fault(testcontrol.FaultCandidateGen); fault != nil {
		_ = s.store.UpdateRunStatus(runID, model.RunFailed, func(r *model.Run) error { r.FaultPoint = "candidate_gen"; return nil })
		return nil, fault
	}
	coord := reconcile.NewCoordinator(s.clock())
	result := coord.Run(snap, workers)

	// Result commit fault.
	if fault := s.fault(testcontrol.FaultResultCommit); fault != nil {
		_ = s.store.UpdateRunStatus(runID, model.RunFailed, func(r *model.Run) error { r.FaultPoint = "result_commit"; return nil })
		return nil, fault
	}

	// Build report.
	byID := make(map[string]*model.Record, len(records))
	for _, r := range records {
		byID[r.ID] = r
	}
	rep := report.Build(runID, s.clock(), rs.Revision, batchIDs, result.MatchGroups, result.Discrepancies, byID)
	summary := rep.Summary

	// Single commit stage: atomically persist the report and transition the run
	// to succeeded. This must not re-enter the store lock, so we use the
	// dedicated CommitRunResults method rather than UpdateRunStatus+callback.
	if err := s.store.CommitRunResults(runID, model.RunSucceeded, result.MatchGroups, result.Discrepancies, summary); err != nil {
		return nil, err
	}
	// Record duplicates discovered during reconciliation.
	_ = s.store.RecordDuplicates(result.Duplicates)

	run, _ = s.store.GetRun(runID)
	return run, nil
}

// CancelRun transitions a running run to cancelled if it has not yet reached a
// terminal state.
func (s *Service) CancelRun(runID string) (*model.Run, error) {
	if err := s.store.UpdateRunStatus(runID, model.RunCancelled, nil); err != nil {
		return nil, err
	}
	return s.store.GetRun(runID)
}

// GetRun returns a run.
func (s *Service) GetRun(runID string) (*model.Run, error) { return s.store.GetRun(runID) }

// ExportReport renders the report for a run as JSON or CSV.
func (s *Service) ExportReport(runID string, format string) ([]byte, error) {
	run, err := s.store.GetRun(runID)
	if err != nil {
		return nil, err
	}
	groups, discreps, summary, err := s.store.GetReport(runID)
	if err != nil {
		return nil, err
	}
	records := s.store.ListRecords(run.Snapshot.BatchIDs)
	byID := make(map[string]*model.Record, len(records))
	for _, r := range records {
		byID[r.ID] = r
	}
	rep := report.Build(runID, s.clock(), run.Snapshot.RuleRevision, run.Snapshot.BatchIDs, groups, discreps, byID)
	_ = summary
	switch format {
	case "csv":
		return rep.CSV()
	default:
		return rep.JSON()
	}
}

// ListBatches returns all batches.
func (s *Service) ListBatches() []*model.Batch { return s.store.ListBatches() }

// ListRuns returns all runs.
func (s *Service) ListRuns() []*model.Run { return s.store.ListRuns() }

func (s *Service) fault(fp testcontrol.FaultPoint) error {
	if s.control == nil {
		return nil
	}
	return s.control.ErrIfFaulted(fp)
}

func fileDigest(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])[:16]
}

// SortedBatchIDs is a small helper used by the API to render stable listings.
func SortedBatchIDs(bs []*model.Batch) []string {
	ids := make([]string, len(bs))
	for i, b := range bs {
		ids[i] = b.ID
	}
	sort.Strings(ids)
	return ids
}
