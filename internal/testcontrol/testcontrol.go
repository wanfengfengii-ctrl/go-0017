// Package testcontrol provides the test control plane: a fixed clock, a
// step-based synchronous task driver, and named fault-injection points. It is
// safe for production builds but has no effect unless a Control instance is
// wired into the services. Tests use it to deterministically advance parsing,
// candidate generation and commit stages, and to construct cancel/concurrent
// retransmit, transaction-failure and out-of-order completion scenarios
// without sleeping or using probabilistic assertions.
package testcontrol

import (
	"errors"
	"sync"
	"time"
)

// FaultPoint names a stage where a test can inject an error.
type FaultPoint string

const (
	FaultParse         FaultPoint = "parse"
	FaultStagingCommit FaultPoint = "staging_commit"
	FaultCandidateGen  FaultPoint = "candidate_gen"
	FaultResultCommit  FaultPoint = "result_commit"
)

// Control holds test knobs: a fixed clock, enabled fault points, and step
// barriers for synchronous stage advancement.
type Control struct {
	mu      sync.Mutex
	now     time.Time
	faults  map[FaultPoint]error
	steps   map[string]chan struct{}
	stepped map[string]bool
}

// New returns a Control anchored at the given time.
func New(now time.Time) *Control {
	return &Control{
		now:     now,
		faults:  make(map[FaultPoint]error),
		steps:   make(map[string]chan struct{}),
		stepped: make(map[string]bool),
	}
}

// Now returns the fixed clock time.
func (c *Control) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance advances the fixed clock.
func (c *Control) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// SetFault arms a fault point to return err when the corresponding stage runs.
func (c *Control) SetFault(fp FaultPoint, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.faults[fp] = err
}

// ClearFault removes a fault.
func (c *Control) ClearFault(fp FaultPoint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.faults, fp)
}

// ClearAllFaults removes all faults.
func (c *Control) ClearAllFaults() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.faults = make(map[FaultPoint]error)
}

// CheckFault returns the injected error for fp, or nil.
func (c *Control) CheckFault(fp FaultPoint) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.faults[fp]
}

// ErrIfFaulted returns the injected error if fp is faulted, else nil. It is the
// primary hook stages call.
func (c *Control) ErrIfFaulted(fp FaultPoint) error {
	if err := c.CheckFault(fp); err != nil {
		return err
	}
	return nil
}

// RegisterStep creates a step barrier that stages block on until released.
func (c *Control) RegisterStep(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.steps[name]; ok {
		return
	}
	c.steps[name] = make(chan struct{})
}

// WaitStep blocks until the named step is released or the stage is not
// registered (returns immediately). Used by stages to make their progress
// externally observable and controllable.
func (c *Control) WaitStep(name string) {
	c.mu.Lock()
	ch, ok := c.steps[name]
	c.mu.Unlock()
	if !ok {
		return
	}
	<-ch
}

// IsStepped reports whether a step was released.
func (c *Control) IsStepped(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stepped[name]
}

// Release lets a step proceed. Subsequent WaitStep calls return immediately.
func (c *Control) Release(name string) {
	c.mu.Lock()
	ch, ok := c.steps[name]
	if !ok {
		c.mu.Unlock()
		return
	}
	if c.stepped[name] {
		c.mu.Unlock()
		return
	}
	c.stepped[name] = true
	close(ch)
	c.mu.Unlock()
}

// ErrFaulted is a sentinel returned when a stage is asked to fault.
var ErrFaulted = errors.New("testcontrol: injected fault")
