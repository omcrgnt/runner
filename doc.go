/*
Package runner starts and stops lifecycle resources wired via sdi.

After [github.com/omcrgnt/sdi.Resolve], [Runner] receives []Starter and []Closer
(registration order). [Runner.Run] starts every starter concurrently.
[Runner.Stop] closes every closer in reverse registration order.

Register *Runner via [github.com/omcrgnt/runner/use] (Fixed on [unique.Global]).
[github.com/omcrgnt/app].App receives Runner through DI and calls Run/Stop.
*/
package runner
