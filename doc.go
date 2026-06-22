/*
Package runner starts and stops lifecycle resources from [res.Registry].

After [sdi.Resolve], [Runner.Run] starts every [Starter] in registration order
(concurrently). [Runner.Stop] closes [StartCloser] first (reverse order), then
passive [Closer] resources.

Register *Runner via [github.com/omcrgnt/builder].Seed (NewResourceer); [github.com/omcrgnt/app].App
receives it through DI and calls Run/Stop on the same registry.
*/
package runner
