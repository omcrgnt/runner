/*
Package runner starts and stops lifecycle resources wired via sdi.

After [github.com/omcrgnt/sdi.Resolve], [Runner] receives []Starter and []Closer
(registration order). [Runner.Run] starts every starter concurrently with a
lifecycle context derived from the run context. The lifecycle context is canceled
on starter failure (fail-fast) or when the parent run context is canceled; it is
not canceled when [Runner.Run] returns after starters that exit Start without
blocking (e.g. background servers). Starters that block until shutdown should wait
on the lifecycle context passed to Start. [Runner.Stop] releases the lifecycle cancel
func and closes every closer in reverse registration order.

Register *Runner via [github.com/omcrgnt/runner/use] (Fixed on [unique.Global]).
[github.com/omcrgnt/app].App receives Runner through DI and calls Run/Stop.
*/
package runner
