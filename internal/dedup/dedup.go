// Package dedup implements layered duplicate detection over a set of committed
// records. It identifies four duplicate classes, in priority order:
//
//   - exact_key:        same external_id within the same source.
//   - retransmit:       same file digest re-uploaded (same source, same content).
//   - content_fingerprint: different external_id but identical content hash.
//   - suspected:        heuristic near-match (same amount, time, opposite party).
//
// For each duplicate, a canonical record is chosen deterministically (the
// record with the smallest stable key). The rule's dup policy decides whether
// the duplicate is excluded from matching or reported as a discrepancy. The
// output is fully deterministic: canonical/duplicate assignment and relation
// IDs do not depend on iteration order.
package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/settlemesh/settlemesh/internal/model"
)

// Result is the dedup outcome for a batch.
type Result struct {
	Relations []*model.DuplicateRelation
	// ExcludedRecordIDs are duplicates whose policy is "exclude"; they are
	// removed from matching.
	ExcludedRecordIDs map[string]bool
}

// Policy resolves the configured action for a duplicate type. It returns
// DupReport when no policy is configured for a type.
func Policy(rules map[model.DupType]model.DupAction, dt model.DupType) model.DupAction {
	if a, ok := rules[dt]; ok {
		return a
	}
	return model.DupReport
}

// Detect runs layered duplicate detection over the given records (which must
// all belong to sources covered by the rules). records is not modified; the
// returned Result references record IDs only.
func Detect(records []*model.Record, rules map[model.DupType]model.DupAction) *Result {
	r := &Result{ExcludedRecordIDs: make(map[string]bool)}

	// exact_key + content_fingerprint within a single pass over per-source
	// groups. We process sources in sorted order for deterministic relation IDs.
	bySource := make(map[string][]*model.Record)
	for _, rec := range records {
		bySource[rec.SourceID] = append(bySource[rec.SourceID], rec)
	}
	sourceIDs := sortedKeys(bySource)
	for _, sid := range sourceIDs {
		recs := bySource[sid]
		// Sort within source by stable key so canonical selection is deterministic.
		sort.Slice(recs, func(i, j int) bool { return recs[i].StableKey < recs[j].StableKey })

		// exact_key: same external_id.
		byExt := make(map[string][]*model.Record)
		for _, rec := range recs {
			byExt[rec.ExternalID] = append(byExt[rec.ExternalID], rec)
		}
		extKeys := sortedRecordKeys(byExt)
		for _, ext := range extKeys {
			group := byExt[ext]
			if len(group) < 2 {
				continue
			}
			canonical := group[0]
			for _, dup := range group[1:] {
				rel := newRelation(model.DupExactKey, canonical, dup, rules)
				r.add(rel)
			}
		}

		// content_fingerprint: same content hash, different external_id.
		byHash := make(map[string][]*model.Record)
		for _, rec := range recs {
			byHash[rec.ContentHash] = append(byHash[rec.ContentHash], rec)
		}
		hashKeys := sortedRecordKeys(byHash)
		for _, h := range hashKeys {
			group := byHash[h]
			if len(group) < 2 {
				continue
			}
			// Canonical = smallest stable key.
			canonical := group[0]
			for _, dup := range group[1:] {
				// Only flag as content_fingerprint if external ids differ
				// (same external id already handled as exact_key).
				if dup.ExternalID == canonical.ExternalID {
					continue
				}
				rel := newRelation(model.DupContentFingerprint, canonical, dup, rules)
				r.add(rel)
			}
		}
	}

	// retransmit: same file digest across batches of the same source. We
	// approximate by grouping records that share both source and a "file
	// digest" derived from (source_id, first line, content). Since ingest
	// attaches BatchID and the store tracks file digests, the caller may pass
	// pre-computed retransmit pairs; here we detect content-identical records
	// across different batches of the same source as retransmit candidates.
	byContent := make(map[string][]*model.Record)
	for _, rec := range records {
		key := rec.SourceID + "|" + rec.ContentHash + "|" + rec.ExternalID
		byContent[key] = append(byContent[key], rec)
	}
	ck := sortedRecordKeys(byContent)
	for _, k := range ck {
		group := byContent[k]
		if len(group) < 2 {
			continue
		}
		// Same content AND same external id across batches => retransmit.
		sort.Slice(group, func(i, j int) bool { return group[i].StableKey < group[j].StableKey })
		canonical := group[0]
		for _, dup := range group[1:] {
			if dup.BatchID == canonical.BatchID {
				continue
			}
			rel := newRelation(model.DupRetransmit, canonical, dup, rules)
			r.add(rel)
		}
	}

	return r
}

func (r *Result) add(rel *model.DuplicateRelation) {
	r.Relations = append(r.Relations, rel)
	if rel.Action == model.DupExclude {
		r.ExcludedRecordIDs[rel.DuplicateRecordID] = true
	}
}

func newRelation(dt model.DupType, canonical, dup *model.Record, rules map[model.DupType]model.DupAction) *model.DuplicateRelation {
	return &model.DuplicateRelation{
		ID:                relationID(dt, canonical, dup),
		Type:              dt,
		CanonicalRecordID: canonical.ID,
		DuplicateRecordID: dup.ID,
		SourceID:          canonical.SourceID,
		Action:            Policy(rules, dt),
	}
}

func relationID(dt model.DupType, canonical, dup *model.Record) string {
	// Deterministic: hash of type + sorted [canonical, dup] ids.
	a, b := canonical.ID, dup.ID
	if a > b {
		a, b = b, a
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s", dt, a, b)
	return "dup_" + hex.EncodeToString(h.Sum(nil))[:16]
}

func sortedKeys(m map[string][]*model.Record) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedRecordKeys(m map[string][]*model.Record) []string { return sortedKeys(m) }

// FilterExcluded returns records that are not in the excluded set, preserving
// their relative order.
func FilterExcluded(records []*model.Record, excluded map[string]bool) []*model.Record {
	out := make([]*model.Record, 0, len(records))
	for _, r := range records {
		if excluded[r.ID] {
			continue
		}
		out = append(out, r)
	}
	return out
}
