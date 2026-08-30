package runner

import "context"

// Starter is a lifecycle resource that [Runner] starts during Run.
//
// Start MUST return promptly after the resource is running (or ready to run in
// the background). It MUST NOT block until shutdown or wait on ctx.Done() as its
// main body. Long-running work belongs in a goroutine (or equivalent) that
// watches the lifecycle context passed to Start; the returned cleanup / context
// cancel stops that work.
//
// Blocking inside Start breaks fail-fast and Stop: Runner only records a
// successful start after Start returns a nil error.
//
// On success, Start returns its own cleanup, called during [Runner.Stop] in
// place of a separately-implemented [Closer] — a type that both starts and
// must be closed returns cleanup from Start instead of also implementing
// Closer, so there is exactly one path tying "how it was opened" to "how it
// is closed". cleanup may be nil if there is nothing to undo. On failure,
// cleanup is ignored (and must be nil).
//
// Run starts every Starter concurrently, with no ordering guarantee within
// that wave — reading another normal Starter's Start-computed state from
// inside your own Start is unsafe. [LastStarter] is the one exception: it
// runs only after every normal Starter's Start has returned, so reading
// normal-Starter state from inside a LastStarter's Start is safe. See the
// lifecycle safety rule in this package's doc.
type Starter interface {
	Start(ctx context.Context) (cleanup func(context.Context) error, err error)
}

// StandBy is a sequential post-Resolve initialization hook: it runs once, in
// registration order, before Run's Start phase, for zero-I/O finishing
// touches that read another resource's Inject-computed state (e.g. building
// an SDK client wrapper around a *clienthttp.Client whose own Inject ran
// later in the same sdi.Resolve pass). Anything that performs real I/O or
// may run long belongs in Starter instead.
//
// On success, StandBy returns its own cleanup, called during [Runner.Stop]
// alongside pure [Closer]s — the same one-path rule as Starter: whatever a
// resource sets up in StandBy, it also tears down via the closure it hands
// back from StandBy, not a separately-implemented method. cleanup may be nil
// if there is nothing to undo (most StandBy resources, e.g. client-ollama,
// which only wraps an already-owned *http.Client, allocate nothing). On
// failure, cleanup is ignored (and must be nil).
//
// Runner retains every returned cleanup for its own whole lifetime, not just
// for the duration of one Run call — this is why StandBy lives here rather
// than in a Bootstrap-scoped caller: a closure local to a single Bootstrap
// call cannot survive to be invoked by a later, ordinary Stop.
type StandBy interface {
	StandBy() (cleanup func(context.Context) error, err error)
}

// Closer is for a resource with neither Start nor StandBy — fully ready
// after Inject/Build (e.g. client-http, client-s3, conn-sql, telemetry) — so
// there is no Start/StandBy call for it to return a cleanup from instead.
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
