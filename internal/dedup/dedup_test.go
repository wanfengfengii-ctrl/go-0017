package dedup

import (
	"testing"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
)

func rec(id, ext, src, hash, batch string) *model.Record {
	return &model.Record{
		ID:          id,
		ExternalID:  ext,
		SourceID:    src,
		ContentHash: hash,
		BatchID:     batch,
		StableKey:   src + "|" + ext + "|" + hash,
		Amount:      money.FromMinor(money.USD, 100),
	}
}

func TestExactKey(t *testing.T) {
	records := []*model.Record{
		rec("r1", "TX1", "processor", "h1", "b1"),
		rec("r2", "TX1", "processor", "h2", "b1"), // same external id -> exact_key dup
	}
	r := Detect(records, map[model.DupType]model.DupAction{
		model.DupExactKey: model.DupExclude,
	})
	if len(r.Relations) != 1 {
		t.Fatalf("got %d relations, want 1", len(r.Relations))
	}
	rel := r.Relations[0]
	if rel.Type != model.DupExactKey {
		t.Errorf("type = %s, want exact_key", rel.Type)
	}
	if rel.CanonicalRecordID != "r1" {
		t.Errorf("canonical = %s, want r1 (smaller stable key)", rel.CanonicalRecordID)
	}
	if rel.DuplicateRecordID != "r2" {
		t.Errorf("dup = %s, want r2", rel.DuplicateRecordID)
	}
	if rel.Action != model.DupExclude {
		t.Errorf("action = %s, want exclude", rel.Action)
	}
	if !r.ExcludedRecordIDs["r2"] {
		t.Error("r2 should be excluded")
	}
	if r.ExcludedRecordIDs["r1"] {
		t.Error("r1 should not be excluded")
	}
}

func TestContentFingerprint(t *testing.T) {
	records := []*model.Record{
		rec("r1", "TX1", "processor", "SAME", "b1"),
		rec("r2", "TX2", "processor", "SAME", "b1"), // same content, diff ext -> content_fingerprint
	}
	r := Detect(records, map[model.DupType]model.DupAction{
		model.DupContentFingerprint: model.DupReport,
	})
	var found bool
	for _, rel := range r.Relations {
		if rel.Type == model.DupContentFingerprint {
			found = true
			if rel.Action != model.DupReport {
				t.Errorf("action = %s, want report", rel.Action)
			}
		}
	}
	if !found {
		t.Fatal("no content_fingerprint relation")
	}
}

func TestRetransmit(t *testing.T) {
	// Same external id + content across two batches => retransmit.
	records := []*model.Record{
		rec("r1", "TX1", "processor", "SAME", "b1"),
		rec("r2", "TX1", "processor", "SAME", "b2"),
	}
	r := Detect(records, map[model.DupType]model.DupAction{})
	var found bool
	for _, rel := range r.Relations {
		if rel.Type == model.DupRetransmit {
			found = true
		}
	}
	if !found {
		t.Fatal("no retransmit relation")
	}
}

func TestDeterministicCanonical(t *testing.T) {
	// Regardless of input order, canonical must be the one with smallest
	// stable key.
	records := []*model.Record{
		rec("z", "TX1", "processor", "h1", "b1"),
		rec("a", "TX1", "processor", "h2", "b1"),
		rec("m", "TX1", "processor", "h3", "b1"),
	}
	// stable keys: processor|TX1|h1, processor|TX1|h2, processor|TX1|h3
	// smallest stable key is the one with h1 = "z" record.
	r1 := Detect(records, nil)
	// shuffle: reverse order
	rev := []*model.Record{records[2], records[1], records[0]}
	r2 := Detect(rev, nil)
	if len(r1.Relations) != len(r2.Relations) {
		t.Fatalf("relation count differs: %d vs %d", len(r1.Relations), len(r2.Relations))
	}
	for i := range r1.Relations {
		if r1.Relations[i].CanonicalRecordID != r2.Relations[i].CanonicalRecordID {
			t.Errorf("relation %d canonical differs: %s vs %s", i, r1.Relations[i].CanonicalRecordID, r2.Relations[i].CanonicalRecordID)
		}
		if r1.Relations[i].ID != r2.Relations[i].ID {
			t.Errorf("relation %d id differs: %s vs %s", i, r1.Relations[i].ID, r2.Relations[i].ID)
		}
	}
	// canonical should be the smallest stable key record: processor|TX1|h1 -> id "z"
	// Actually stable key uses hash h1<h2<h3, so canonical id "z".
	if r1.Relations[0].CanonicalRecordID != "z" {
		t.Errorf("canonical = %s, want z", r1.Relations[0].CanonicalRecordID)
	}
}

func TestFilterExcluded(t *testing.T) {
	records := []*model.Record{
		rec("r1", "TX1", "s", "h1", "b1"),
		rec("r2", "TX2", "s", "h2", "b1"),
		rec("r3", "TX3", "s", "h3", "b1"),
	}
	excluded := map[string]bool{"r2": true}
	out := FilterExcluded(records, excluded)
	if len(out) != 2 {
		t.Fatalf("got %d, want 2", len(out))
	}
	if out[0].ID != "r1" || out[1].ID != "r3" {
		t.Errorf("filtered order wrong")
	}
}
