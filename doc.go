/*
Package runner runs the StandBy phase, starts and stops lifecycle resources
wired via sdi.

After [github.com/omcrgnt/sdi.Resolve], [Runner] receives []StandBy,
[]Starter and []Closer (registration order). [Runner.Run] runs the StandBy
phase, then starts every [Starter] concurrently with a lifecycle context
derived from the run context. The lifecycle context is canceled on starter
failure (fail-fast) or when the parent run context is canceled; it is not
canceled when [Runner.Run] returns after starters that exit Start without
blocking (e.g. background servers).

[StandBy.StandBy] runs once per resource, sequentially, in registration
order, before the Start phase — for zero-I/O finishing touches that read
another resource's Inject-computed state (e.g. building an SDK client
wrapper around a *clienthttp.Client whose own Inject ran later in the same
sdi.Resolve pass). Anything that performs real I/O or may run long belongs
in Starter instead.

[Starter.Start] must return promptly: spawn background work if needed and watch
the lifecycle context there. Do not block inside Start until shutdown.

Starters run in two waves: every [Starter] that is not also a [LastStarter]
first, concurrently, with no ordering between them; only once all of those
have returned successfully does the second wave — every [LastStarter] — start,
also concurrently with each other. [Gate] opens once both waves have fully
succeeded, letting middleware/interceptors elsewhere (e.g. srv-http, srv-grpc)
hold off real traffic until then; it never opens if either wave fails.

Both StandBy and Start return their own cleanup, a func(context.Context)
error, in place of a separately-implemented Closer: whatever a resource sets
up in StandBy or Start, it also tears down via the closure it hands back
from that same call, not a second, independently-maintained method. This is
why StandBy lives here rather than in a Bootstrap-scoped caller: Runner
lives for the whole process, so it can retain that closure until Stop,
unlike a closure local to a single Bootstrap call. [Closer] remains for
resources with neither Start nor StandBy (e.g. client-http, client-s3,
conn-sql, telemetry) — fully ready after Inject/Build, so there is no
Start/StandBy call for them to return a cleanup from instead.

If a StandBy call fails, Run unwinds every earlier-succeeded StandBy's
cleanup (reverse order) and returns the failure joined with any cleanup
errors — the Start phase never begins. If a Starter's Start fails (either
wave), Run unwinds every already-started Starter's cleanup (reverse order,
concurrently) and every StandBy's cleanup (reverse order), joining all of
that into the returned error.

[Runner.Stop] releases the lifecycle cancel func, then closes, in order:

  - every started Starter's cleanup (reverse registration order, concurrently);
  - every StandBy's cleanup together with every pure [Closer] (each reverse
    registration order; relative order between the two lists does not matter).

A cleanup already consumed by Run's own unwind on failure is not invoked
again by a later Stop. Starters whose Start failed are not closed. There is
no lifecycle dependency graph in sdi; close order is registration order
only, not a reverse topo.

Register *Runner via [github.com/omcrgnt/runner/use] (Fixed on [unique.Global]).
[github.com/omcrgnt/app].App receives Runner through DI and calls Run/Stop.

Breaking v0.23: Runner now requires a gateOpener-compatible resource in the
registry (see [Gate]) — sdi.Resolve fails without one. Real deployments get
this for free: [Gate] registers itself on [unique.Global] via this package's
own init, the same way Runner itself is typically registered. Only a
hand-built registry (e.g. an isolated unique.New() in a test) needs to add
one explicitly.

Breaking v0.24: [Starter.Start] returns (cleanup, error) instead of error;
[StandBy] moved here from github.com/omcrgnt/app, and also returns
(cleanup, error) instead of error.
*/
package runner
