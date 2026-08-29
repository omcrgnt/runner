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
