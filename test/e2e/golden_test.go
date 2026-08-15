package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/settlemesh/settlemesh/internal/reconcile"
	"github.com/settlemesh/settlemesh/internal/report"
)

// TestGoldenReport verifies that the report produced from the fixed samples
// matches a committed golden file byte for byte. Run with -update to refresh
// the golden file after an intentional change.
func TestGoldenReport(t *testing.T) {
	fix := ingestSamples(t)
	snap := &reconcile.Snapshot{RunID: "run1", Records: fix.records, RuleSet: fix.ruleSet, BatchIDs: []string{"b-internal", "b-processor", "b-bank"}}
	r := reconcile.NewCoordinator(fixedTime()).Run(snap, 4)
	rep := report.Build("run1", fixedTime(), fix.ruleSet.Revision, snap.BatchIDs, r.MatchGroups, r.Discrepancies, fix.byID)
	got, err := rep.JSON()
	if err != nil {
		t.Fatal(err)
	}

	goldenPath := filepath.Join("testdata", "golden_report.json")
	if *updateFlag {
		if err := os.MkdirAll("testdata", 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, got, 0644); err != nil {
			t.Fatal(err)
		}
		t.Logf("updated golden file %s", goldenPath)
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("golden file missing; run `go test -run TestGoldenReport -update` to create it: %v", err)
	}
	if string(want) != string(got) {
		t.Errorf("report does not match golden file:\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

var updateFlag = flagBool("update", false, "regenerate golden files")

// flagBool wraps testing flags without importing flag in a confusing way.
func flagBool(name string, def bool, usage string) *bool {
	return boolFlag(name, def, usage)
}
