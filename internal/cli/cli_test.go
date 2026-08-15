package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/store"
)

func newCLI(t *testing.T) (*CLI, *bytes.Buffer) {
	t.Helper()
	s := store.New(func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) })
	svc := service.New(s)
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
	svc.PutRuleSet(&model.RuleSet{Revision: 1, Rules: []*model.Rule{{
		ID:           "r1",
		AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank},
		BusinessIDFields: map[model.Role]string{
			model.RoleInternal:  model.FieldBusinessID,
			model.RoleProcessor: model.FieldBusinessID,
			model.RoleBank:      model.FieldBusinessID,
		},
		AmountTolerance: money.NewAbsTolerance(5),
		FeeTolerance:    money.NewAbsTolerance(0),
		TimeWindow:      int64(60 * time.Second),
	}}})
	buf := &bytes.Buffer{}
	cli := New(svc)
	cli.SetOutput(buf)
	return cli, buf
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCLIFlow(t *testing.T) {
	cli, buf := newCLI(t)
	dir := t.TempDir()

	// source-add
	srcPath := writeFile(t, dir, "src.json", `{"source_id":"internal","name":"internal","role":"internal","currency":"USD","timezone":"UTC","format":"csv","field_map":{"external_id":"external_id","amount":"amount","timestamp":"timestamp","direction":"direction","business_id":"business_id"}}`)
	if code := cli.Run([]string{"settlemesh", "source-add", srcPath}); code != 0 {
		t.Fatalf("source-add: code %d out %s", code, buf.String())
	}

	// batch-submit (internal)
	csv := "external_id,amount,timestamp,direction,business_id\nI1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	feedPath := writeFile(t, dir, "internal.csv", csv)
	buf.Reset()
	if code := cli.Run([]string{"settlemesh", "batch-submit", "--source", "internal", "--key", "k1", feedPath}); code != 0 {
		t.Fatalf("batch-submit: code %d out %s", code, buf.String())
	}
	// Extract the batch ID from the JSON output.
	internalBatchID := extractID(buf.String(), "batch_id")
	if internalBatchID == "" {
		t.Fatalf("could not parse batch id from %s", buf.String())
	}

	// batch-submit (processor)
	csv2 := "external_id,amount,timestamp,direction,business_id\nP1,100.00,2026-01-02T03:04:05Z,credit,B1\n"
	feedPath2 := writeFile(t, dir, "processor.csv", csv2)
	buf.Reset()
	cli.Run([]string{"settlemesh", "batch-submit", "--source", "processor", "--key", "k2", feedPath2})
	processorBatchID := extractID(buf.String(), "batch_id")

	// run-start
	buf.Reset()
	code := cli.Run([]string{"settlemesh", "run-start", "--id", "run1", "--batches", internalBatchID + "," + processorBatchID})
	if code != 0 {
		t.Fatalf("run-start: code %d out %s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "succeeded") {
		t.Errorf("run-start output: %s", buf.String())
	}

	// run-list
	buf.Reset()
	cli.Run([]string{"settlemesh", "run-list"})
	if !strings.Contains(buf.String(), "run1") {
		t.Errorf("run-list output: %s", buf.String())
	}

	// run-report
	buf.Reset()
	cli.Run([]string{"settlemesh", "run-report", "run1"})
	if !strings.Contains(buf.String(), "match_groups") {
		t.Errorf("run-report output: %s", buf.String())
	}

	// batch-list
	buf.Reset()
	cli.Run([]string{"settlemesh", "batch-list"})
	if !strings.Contains(buf.String(), internalBatchID) {
		t.Errorf("batch-list output: %s", buf.String())
	}
}

func TestCLIUsage(t *testing.T) {
	cli, _ := newCLI(t)
	if code := cli.Run([]string{"settlemesh"}); code != 2 {
		t.Errorf("no args: code %d, want 2", code)
	}
	if code := cli.Run([]string{"settlemesh", "help"}); code != 0 {
		t.Errorf("help: code %d, want 0", code)
	}
}

func extractID(j, field string) string {
	// crude: find "field":  then the next quoted string.
	needle := "\"" + field + "\":"
	idx := strings.Index(j, needle)
	if idx < 0 {
		return ""
	}
	rest := j[idx+len(needle):]
	// skip whitespace
	for len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\n' || rest[0] == '\t' || rest[0] == '\r') {
		rest = rest[1:]
	}
	if len(rest) == 0 || rest[0] != '"' {
		return ""
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}
