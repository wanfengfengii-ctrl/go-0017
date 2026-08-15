// Package reconcile is the parallel reconciliation coordinator. It freezes an
// input snapshot (committed batch IDs, batch summaries, rule revision),
// computes deduplication, fans candidate generation across disjoint
// partitions, and then performs a single deterministic commit stage that
// merges results by stable partition key. The output is identical for any
// scheduling: partitions are processed in sorted order and every ID is a hash
// of sorted member record IDs, so goroutine completion order cannot affect the
// result.
package reconcile

import (
	"sort"
	"sync"
	"time"

	"github.com/settlemesh/settlemesh/internal/dedup"
	"github.com/settlemesh/settlemesh/internal/matching"
	"github.com/settlemesh/settlemesh/internal/model"
)

// Coordinator orchestrates a reconciliation run.
type Coordinator struct {
	now time.Time
}

// NewCoordinator constructs a coordinator. now is the logical run time (use a
// fixed clock in tests).
func NewCoordinator(now time.Time) *Coordinator {
	return &Coordinator{now: now}
}

// Snapshot is the frozen input to a run.
type Snapshot struct {
	RunID          string
	BatchIDs       []string
	Records        []*model.Record
	RuleSet        *model.RuleSet
	BatchSummaries []model.BatchSummary
}

// Result is the committed run output.
type Result struct {
	RunID         string
	MatchGroups   []*model.MatchGroup
	Discrepancies []*model.Discrepancy
	Duplicates    []*model.DuplicateRelation
	ExcludedIDs   map[string]bool
	Snapshot      *Snapshot
}

// Run executes the reconciliation. workers controls the parallelism of
// candidate generation across partitions; results are identical for any
// workers >= 1.
func (c *Coordinator) Run(snap *Snapshot, workers int) *Result {
	if workers < 1 {
		workers = 1
	}

	// Layered dedup over all records, using the first rule's dup policy (rules
	// are assumed to share a dup policy for a given run).
	dupPolicy := map[model.DupType]model.DupAction{}
	if len(snap.RuleSet.Rules) > 0 {
		dupPolicy = snap.RuleSet.Rules[0].DupPolicy
	}
	dupResult := dedup.Detect(snap.Records, dupPolicy)
	excluded := dupResult.ExcludedRecordIDs

	// Partition records by currency. Partitions are disjoint, so candidate
	// generation can run concurrently. Duplicate relations are recorded for
	// every record, not just per-partition.
	partitions := partitionByCurrency(snap.Records, excluded)
	partKeys := sortedPartitionKeys(partitions)

	// Concurrent candidate generation: each partition produces its own
	// matching.Result. Because partitions are disjoint and IDs are content
	// hashes, the union is order-independent.
	type partResult struct {
		key string
		res *matching.Result
	}
	results := make([]*partResult, len(partKeys))

	type job struct {
		idx  int
		key  string
		recs []*model.Record
	}
	jobs := make([]job, len(partKeys))
	for i, k := range partKeys {
		jobs[i] = job{idx: i, key: k, recs: partitions[k]}
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			engine := matching.NewEngine(snap.RuleSet.Rules, nil, c.now)
			r := engine.Run(j.recs, excluded)
			mu.Lock()
			results[j.idx] = &partResult{key: j.key, res: r}
			mu.Unlock()
		}(j)
	}
	wg.Wait()

	// Single commit stage: merge partition results in stable (sorted) partition
	// key order. This ordering makes the final slice deterministic.
	res := &Result{
		RunID:       snap.RunID,
		ExcludedIDs: excluded,
		Duplicates:  dupResult.Relations,
		Snapshot:    snap,
	}
	for _, pr := range results {
		if pr == nil {
			continue
		}
		res.MatchGroups = append(res.MatchGroups, pr.res.Groups...)
		res.Discrepancies = append(res.Discrepancies, pr.res.Discrepancies...)
	}
	// Sort groups and discrepancies deterministically as a final guarantee
	// (they are already produced in partition order, but sort makes it robust).
	sort.Slice(res.MatchGroups, func(i, j int) bool {
		return res.MatchGroups[i].ID < res.MatchGroups[j].ID
	})
	sort.Slice(res.Discrepancies, func(i, j int) bool {
		if res.Discrepancies[i].Type != res.Discrepancies[j].Type {
			return res.Discrepancies[i].Type < res.Discrepancies[j].Type
		}
		return res.Discrepancies[i].ID < res.Discrepancies[j].ID
	})
	sort.Slice(res.Duplicates, func(i, j int) bool {
		return res.Duplicates[i].ID < res.Duplicates[j].ID
	})
	return res
}

func partitionByCurrency(records []*model.Record, excluded map[string]bool) map[string][]*model.Record {
	out := make(map[string][]*model.Record)
	for _, r := range records {
		if r.Isolated {
			key := r.Amount.C.Code
			out[key] = append(out[key], r)
			continue
		}
		if excluded[r.ID] {
			continue
		}
		key := r.Amount.C.Code
		out[key] = append(out[key], r)
	}
	return out
}

func sortedPartitionKeys(m map[string][]*model.Record) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
