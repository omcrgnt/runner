package runner

import (
	"sync"

	"github.com/omcrgnt/res/unique"
)

// Gate signals that both Starter waves have finished. Open is called once
// by Runner.Run after the second wave succeeds; Ready is a non-blocking
// check for anyone gating on it (e.g. srv-http/srv-grpc middleware,
// deciding whether to serve a request or answer 503/Unavailable).
type Gate struct {
	ch   chan struct{}
	once sync.Once
}

// Open is idempotent: *Gate is a fixed singleton (unique.MustAddFixed) and
// nothing stops a second Runner.Run on the same process — without this,
// that second call's Open would panic on an already-closed channel.
func (g *Gate) Open() { g.once.Do(func() { close(g.ch) }) }

func (g *Gate) Ready() bool {
	select {
	case <-g.ch:
		return true
	default:
		return false
	}
}

var _ gateOpener = (*Gate)(nil)

func init() {
	unique.MustAddFixed(&Gate{ch: make(chan struct{})})
}
