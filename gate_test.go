package runner

import "testing"

func TestGate_ReadyBeforeAndAfterOpen(t *testing.T) {
	g := &Gate{ch: make(chan struct{})}
	if g.Ready() {
		t.Fatal("expected Ready() == false before Open")
	}
	g.Open()
	if !g.Ready() {
		t.Fatal("expected Ready() == true after Open")
	}
}

func TestGate_Open_secondCallDoesNotPanic(t *testing.T) {
	// *Gate is a fixed singleton; nothing stops a second Runner.Run on the
	// same process from calling Open twice.
	g := &Gate{ch: make(chan struct{})}
	g.Open()
	g.Open()
	if !g.Ready() {
		t.Fatal("expected Ready() == true after a second Open")
	}
}
