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
