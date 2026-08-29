package runner

import "context"

// Starter is a lifecycle resource that [Runner] starts during Run.
//
// Start MUST return promptly after the resource is running (or ready to run in
// the background). It MUST NOT block until shutdown or wait on ctx.Done() as its
// main body. Long-running work belongs in a goroutine (or equivalent) that
// watches the lifecycle context passed to Start; [Closer.Close] / context cancel
// stops that work.
//
// Blocking inside Start breaks fail-fast and Stop: Runner only records a
// successful start after Start returns nil.
//
// Run starts every Starter concurrently, with no ordering between them —
// reading another resource's Start-computed state from inside your own Start
// is unsafe. See the lifecycle safety rule in [github.com/omcrgnt/app]'s
// package doc.
type Starter interface {
	Start(ctx context.Context) error
}

// Closer is a lifecycle resource that [Runner] stops during Stop.
type Closer interface {
	Close(ctx context.Context) error
}

// LastStarter is a Starter that must run only after every other Starter's
// Start has returned successfully — e.g. the ops/readiness server, whose
// own readiness checks read state that other Starters write during Start.
// Implement it by adding LastStart() (a marker; its body is never called)
// alongside the existing Start method. Run partitions r.starters by this
// marker locally — it is not requested as a separate sdi dependency, and
// any number of Starters may implement it (they run concurrently with each
// other in the second wave, same no-ordering-guarantee as the first).
type LastStarter interface {
	Starter
	LastStart()
}
