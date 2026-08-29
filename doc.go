/*
Package runner starts and stops lifecycle resources wired via sdi.

After [github.com/omcrgnt/sdi.Resolve], [Runner] receives []Starter and []Closer
(registration order). [Runner.Run] starts every starter concurrently with a
lifecycle context derived from the run context. The lifecycle context is canceled
on starter failure (fail-fast) or when the parent run context is canceled; it is
not canceled when [Runner.Run] returns after starters that exit Start without
blocking (e.g. background servers).

[Starter.Start] must return promptly: spawn background work if needed and watch
the lifecycle context there. Do not block inside Start until shutdown.

Starters run in two waves: every [Starter] that is not also a [LastStarter]
first, concurrently, with no ordering between them; only once all of those
have returned successfully does the second wave — every [LastStarter] — start,
also concurrently with each other. [Gate] opens once both waves have fully
succeeded, letting middleware/interceptors elsewhere (e.g. srv-http, srv-grpc)
hold off real traffic until then; it never opens if either wave fails.

[Runner.Stop] releases the lifecycle cancel func, then closes:

  - each successfully started starter, either wave, that also implements [Closer] (registration order);
  - each pure [Closer] (in closers but not also a starter), e.g. DB pools without Start.

Starters whose Start failed are not closed. There is no lifecycle dependency graph
in sdi; close order is registration order only, not a reverse topo.

Register *Runner via [github.com/omcrgnt/runner/use] (Fixed on [unique.Global]).
[github.com/omcrgnt/app].App receives Runner through DI and calls Run/Stop.
*/
package runner
