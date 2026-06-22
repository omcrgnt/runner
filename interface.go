package runner

import "context"

type Starter interface {
	Start(ctx context.Context) error
}

type Closer interface {
	Close(ctx context.Context) error
}

type StartCloser interface {
	Starter
	Closer
}
