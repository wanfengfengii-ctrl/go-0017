package e2e

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/settlemesh/settlemesh/internal/ingest"
	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/money"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/store"
	"github.com/settlemesh/settlemesh/internal/testcontrol"
)

var fixedTime = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func source(role model.Role, format string) *model.Source {
	return &model.Source{
		ID: string(role), Name: string(role), Role: role, Currency: "USD",
		TimeZone: "UTC", Format: format,
		FieldMap: map[string]string{
			model.FieldExternalID: model.FieldExternalID,
			model.FieldAmount:     model.FieldAmount,
			model.FieldTimestamp:  model.FieldTimestamp,
			model.FieldFee:        model.FieldFee,
			model.FieldDirection:  model.FieldDirection,
			model.FieldBusinessID: model.FieldBusinessID,
		},
	}
}

func exampleRuleSet() *model.RuleSet {
	abs5 := int64(5)
	zero := int64(0)
	return &model.RuleSet{
		Revision: 1,
		Rules: []*model.Rule{
			{
				ID:           "main",
				Name:         "three-way",
				AllowedRoles: []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank},
				BusinessIDFields: map[model.Role]string{
					model.RoleInternal:  model.FieldBusinessID,
					model.RoleProcessor: model.FieldBusinessID,
					model.RoleBank:      model.FieldBusinessID,
				},
				AmountTolerance: money.Tolerance{Abs: &abs5},
				FeeTolerance:    money.Tolerance{Abs: &zero},
				TimeWindow:      int64(120 * time.Second),
				DupPolicy: map[model.DupType]model.DupAction{
					model.DupExactKey:           model.DupExclude,
					model.DupRetransmit:         model.DupReport,
					model.DupContentFingerprint: model.DupReport,
				},
				SearchLimit: 10000,
			},
		},
	}
}

// fixture holds parsed records and supporting state for direct reconciliation.
type fixture struct {
	records []*model.Record
	ruleSet *model.RuleSet
	byID    map[string]*model.Record
}

// ingestSamples parses the three sample feeds into records.
func ingestSamples(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{ruleSet: exampleRuleSet(), byID: map[string]*model.Record{}}
	for _, s := range []struct {
		role model.Role
		name string
		bid  string
	}{
		{model.RoleInternal, "internal.csv", "b-internal"},
		{model.RoleProcessor, "processor.csv", "b-processor"},
		{model.RoleBank, "bank.csv", "b-bank"},
	} {
		data := readSample(t, s.name)
		p := ingest.New(ingest.Options{Source: source(s.role, "csv"), Workers: 4, Registry: money.NewRegistry()})
		err := p.Run(strings.NewReader(data), func(res ingest.RowResult) error {
			if res.Record != nil {
				res.Record.BatchID = s.bid
				f.records = append(f.records, res.Record)
				f.byID[res.Record.ID] = res.Record
			}
			return nil
		})
		if err != nil {
			t.Fatalf("ingest %s: %v", s.name, err)
		}
	}
	return f
}

// ingestString parses a single inline feed and returns the valid records.
func ingestString(t *testing.T, role model.Role, feed string, policy model.RowErrorPolicy) []*model.Record {
	t.Helper()
	p := ingest.New(ingest.Options{Source: source(role, "csv"), Workers: 1, Registry: money.NewRegistry()})
	var valid []*model.Record
	err := p.Run(strings.NewReader(feed), func(res ingest.RowResult) error {
		if res.Record != nil && !res.Record.Isolated {
			valid = append(valid, res.Record)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if policy == model.RowPolicyAtomic {
		// Check whether any row had an error; atomic rejects all on error.
		hadErr := false
		p2 := ingest.New(ingest.Options{Source: source(role, "csv"), Workers: 1, Registry: money.NewRegistry()})
		_ = p2.Run(strings.NewReader(feed), func(res ingest.RowResult) error {
			if res.Error != nil {
				hadErr = true
			}
			return nil
		})
		if hadErr {
			return nil
		}
	}
	return valid
}

// newService builds a service with the three sources and example rule set.
func newService(t *testing.T) *service.Service {
	t.Helper()
	svc := service.New(store.New(fixedTime), service.WithClock(fixedTime))
	for _, role := range []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank} {
		if err := svc.AddSource(source(role, "csv")); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.PutRuleSet(exampleRuleSet()); err != nil {
		t.Fatal(err)
	}
	return svc
}

// controlledService wires a test control plane into the service.
type controlledService struct {
	svc     *service.Service
	store   store.Store
	control *testcontrol.Control
}

func newControlledService(t *testing.T) *controlledService {
	t.Helper()
	ctrl := testcontrol.New(fixedTime())
	s := store.New(fixedTime)
	svc := service.New(s, service.WithClock(fixedTime), service.WithControl(ctrl))
	for _, role := range []model.Role{model.RoleInternal, model.RoleProcessor, model.RoleBank} {
		svc.AddSource(source(role, "csv"))
	}
	svc.PutRuleSet(exampleRuleSet())
	return &controlledService{svc: svc, store: s, control: ctrl}
}

func (c *controlledService) submitSample(role model.Role, name, key string) string {
	data := readSampleT(name)
	b, err := c.svc.SubmitAndCommit(string(role), key, "csv", strings.NewReader(data), model.RowPolicyIsolate)
	if err != nil {
		panic(err)
	}
	return b.ID
}
func (c *controlledService) batchIDs() []string {
	bs := c.svc.ListBatches()
	ids := make([]string, 0, len(bs))
	for _, b := range bs {
		if b.Status == model.BatchCommitted {
			ids = append(ids, b.ID)
		}
	}
	return ids
}

// readSampleT is a non-test helper for use outside *testing.T contexts.
func readSampleT(name string) string {
	return mustReadFile(filepath.Join("..", "..", "samples", name))
}

func submitSample(t *testing.T, svc *service.Service, role, name, key string) string {
	data := readSample(t, name)
	b, err := svc.SubmitAndCommit(role, key, "csv", strings.NewReader(data), model.RowPolicyIsolate)
	if err != nil {
		t.Fatal(err)
	}
	return b.ID
}

// fault helpers for the fault-injection test.
func reconcileFault() testcontrol.FaultPoint { return testcontrol.FaultResultCommit }
func reconcileFaultErr() error               { return testcontrol.ErrFaulted }
