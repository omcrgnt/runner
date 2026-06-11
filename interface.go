package runner

import (
	"context"
	"reflect"
)

// Pool — источник ресурсов для Run/Stop; совместим с res.Default (duck typing).
type Pool interface {
	Walk(fn func(t reflect.Type, res any) bool)
}

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
