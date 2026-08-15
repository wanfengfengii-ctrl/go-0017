// Package cli implements the SettleMesh command-line interface. It drives the
// same service layer as the HTTP API: managing sources and rule sets,
// submitting import feeds, starting reconciliation runs and exporting
// reports. The CLI is a thin wrapper over service.Service so behavior is
// identical between the two front-ends.
package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/settlemesh/settlemesh/internal/model"
	"github.com/settlemesh/settlemesh/internal/service"
	"github.com/settlemesh/settlemesh/internal/store"
)

// CLI holds shared configuration for command execution.
type CLI struct {
	svc *service.Service
	out io.Writer
}

// New constructs a CLI over a service. If svc is nil a default in-memory service
// is constructed.
func New(svc *service.Service) *CLI {
	if svc == nil {
		svc = service.New(store.New(time.Now))
	}
	return &CLI{svc: svc, out: os.Stdout}
}

// SetOutput redirects CLI output (used in tests).
func (c *CLI) SetOutput(w io.Writer) { c.out = w }

// Run parses argv and dispatches. It returns an exit code.
func (c *CLI) Run(args []string) int {
	if len(args) < 2 {
		c.usage()
		return 2
	}
	cmd := args[1]
	rest := args[2:]
	var err error
	switch cmd {
	case "source-add":
		err = c.sourceAdd(rest)
	case "ruleset-put":
		err = c.rulesetPut(rest)
	case "batch-submit":
		err = c.batchSubmit(rest)
	case "batch-list":
		err = c.batchList()
	case "run-start":
		err = c.runStart(rest)
	case "run-list":
		err = c.runList()
	case "run-get":
		err = c.runGet(rest)
	case "run-report":
		err = c.runReport(rest)
	case "run-cancel":
		err = c.runCancel(rest)
	case "help", "-h", "--help":
		c.usage()
		return 0
	default:
		fmt.Fprintf(c.out, "unknown command: %s\n", cmd)
		c.usage()
		return 2
	}
	if err != nil {
		fmt.Fprintf(c.out, "error: %v\n", err)
		return 1
	}
	return 0
}

func (c *CLI) usage() {
	fmt.Fprintln(c.out, `settlemesh - multi-source payment reconciliation

commands:
  source-add <source.json>                      register a source
  ruleset-put <ruleset.json>                    put a rule set revision
  batch-submit --source <id> --key <k> [--format csv] [--policy isolate] <feed.csv>
                                                submit and commit an import batch
  batch-list                                    list batches
  run-start --id <run> --batches <b1,b2> [--rule N] [--workers W]
                                                start a reconciliation run
  run-list                                      list runs
  run-get <run-id>                              show a run
  run-report <run-id> [--format json|csv]       export a report
  run-cancel <run-id>                           cancel a run`)
}

func (c *CLI) sourceAdd(args []string) error {
	if len(args) < 1 {
		return errors.New("source-add: missing file")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	var src model.Source
	if err := json.Unmarshal(data, &src); err != nil {
		return err
	}
	if err := c.svc.AddSource(&src); err != nil {
		return err
	}
	return c.printJSON(src)
}

func (c *CLI) rulesetPut(args []string) error {
	if len(args) < 1 {
		return errors.New("ruleset-put: missing file")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	var rs model.RuleSet
	if err := json.Unmarshal(data, &rs); err != nil {
		return err
	}
	if err := c.svc.PutRuleSet(&rs); err != nil {
		return err
	}
	return c.printJSON(rs)
}

type batchSubmitArgs struct {
	source string
	key    string
	format string
	policy string
}

func (c *CLI) batchSubmit(args []string) error {
	a := batchSubmitArgs{format: "csv", policy: "isolate"}
	var feedPath string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--source":
			i++
			a.source = args[i]
		case "--key":
			i++
			a.key = args[i]
		case "--format":
			i++
			a.format = args[i]
		case "--policy":
			i++
			a.policy = args[i]
		default:
			feedPath = args[i]
		}
	}
	if a.source == "" || a.key == "" || feedPath == "" {
		return errors.New("batch-submit: --source, --key and a feed path are required")
	}
	f, err := os.Open(feedPath)
	if err != nil {
		return err
	}
	defer f.Close()
	policy := model.RowPolicyIsolate
	if a.policy == "atomic" {
		policy = model.RowPolicyAtomic
	}
	b, err := c.svc.SubmitAndCommit(a.source, a.key, a.format, f, policy)
	if err != nil {
		return err
	}
	return c.printJSON(b)
}

func (c *CLI) batchList() error {
	return c.printJSON(c.svc.ListBatches())
}

type runStartArgs struct {
	id      string
	batches []string
	rule    int64
	workers int
}

func (c *CLI) runStart(args []string) error {
	a := runStartArgs{workers: 4}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--id":
			i++
			a.id = args[i]
		case "--batches":
			i++
			a.batches = strings.Split(args[i], ",")
		case "--rule":
			i++
			n, _ := strconv.ParseInt(args[i], 10, 64)
			a.rule = n
		case "--workers":
			i++
			n, _ := strconv.Atoi(args[i])
			a.workers = n
		}
	}
	if a.id == "" || len(a.batches) == 0 {
		return errors.New("run-start: --id and --batches are required")
	}
	run, err := c.svc.StartRun(a.id, a.batches, a.rule, a.workers)
	if err != nil {
		return err
	}
	return c.printJSON(run)
}

func (c *CLI) runList() error { return c.printJSON(c.svc.ListRuns()) }

func (c *CLI) runGet(args []string) error {
	if len(args) < 1 {
		return errors.New("run-get: missing run id")
	}
	run, err := c.svc.GetRun(args[0])
	if err != nil {
		return err
	}
	return c.printJSON(run)
}

func (c *CLI) runReport(args []string) error {
	if len(args) < 1 {
		return errors.New("run-report: missing run id")
	}
	format := "json"
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		if rest[i] == "--format" && i+1 < len(rest) {
			format = rest[i+1]
		}
	}
	data, err := c.svc.ExportReport(args[0], format)
	if err != nil {
		return err
	}
	_, err = c.out.Write(data)
	return err
}

func (c *CLI) runCancel(args []string) error {
	if len(args) < 1 {
		return errors.New("run-cancel: missing run id")
	}
	run, err := c.svc.CancelRun(args[0])
	if err != nil {
		return err
	}
	return c.printJSON(run)
}

func (c *CLI) printJSON(v interface{}) error {
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
