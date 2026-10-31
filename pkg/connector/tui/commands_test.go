package tui

import (
	"testing"

	"github.com/mainplane-ai/mainplane/pkg/harness"
	"github.com/mainplane-ai/mainplane/pkg/statefile"
)

func TestContinueAsWaitsWhileStepping(t *testing.T) {
	a := &App{Status: &Text{}}
	s := &session{info: harness.Info{Status: statefile.StatusStepping}}

	s.continueAs(a, statefile.Conf{}, nil)

	const want = "Wait for this session to finish before continuing"
	if a.Status.S != want {
		t.Fatalf("status = %q, want %q", a.Status.S, want)
	}
}
