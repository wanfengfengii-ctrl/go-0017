package testcontrol

import (
	"errors"
	"testing"
	"time"
)

func TestClock(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c := New(base)
	if !c.Now().Equal(base) {
		t.Fatal("clock not fixed")
	}
	c.Advance(5 * time.Second)
	if !c.Now().Equal(base.Add(5 * time.Second)) {
		t.Fatal("advance failed")
	}
}

func TestFaults(t *testing.T) {
	c := New(time.Now())
	if err := c.ErrIfFaulted(FaultParse); err != nil {
		t.Fatal("no fault should be set")
	}
	c.SetFault(FaultParse, errors.New("boom"))
	if err := c.ErrIfFaulted(FaultParse); err == nil || err.Error() != "boom" {
		t.Fatalf("fault not propagated: %v", err)
	}
	c.ClearFault(FaultParse)
	if err := c.ErrIfFaulted(FaultParse); err != nil {
		t.Fatal("fault not cleared")
	}
}

func TestSteps(t *testing.T) {
	c := New(time.Now())
	c.RegisterStep("candidate_gen")
	if c.IsStepped("candidate_gen") {
		t.Fatal("step should not be released yet")
	}
	done := make(chan struct{})
	go func() {
		c.WaitStep("candidate_gen")
		close(done)
	}()
	c.Release("candidate_gen")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("step did not release")
	}
	if !c.IsStepped("candidate_gen") {
		t.Fatal("step should be marked released")
	}
	// Releasing again is a no-op.
	c.Release("candidate_gen")
}
