package runner

import "github.com/omcrgnt/res/unique"

// Gate signals that every normal-wave Starter has finished. Open is called
// once by Runner.Run after the first errgroup.Wait succeeds; Ready is a
// non-blocking check for anyone gating on it (e.g. srv-http/srv-grpc
// middleware, deciding whether to serve a request or answer 503/Unavailable).
type Gate struct {
	ch chan struct{}
}

func (g *Gate) Open() { close(g.ch) }

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
