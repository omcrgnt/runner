package runner

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockResource struct {
	id        string
	startErr  error
	closeErr  error
	isStarted bool
	closedAt  time.Time
}

type activeResource struct{ *mockResource }

func (m *activeResource) Start(ctx context.Context) error {
	m.isStarted = true
	if m.startErr != nil {
		return m.startErr
	}
	<-ctx.Done()
	return nil
}

func (m *activeResource) Close(ctx context.Context) error {
	m.closedAt = time.Now()
	return m.closeErr
}

type passiveResource struct{ *mockResource }

func (m *passiveResource) Close(ctx context.Context) error {
	m.closedAt = time.Now()
	return m.closeErr
}

// immediateStarter returns from Start without blocking (like srv-http today).
type immediateStarter struct {
	mockResource
	lifecycle context.Context
}

func (m *immediateStarter) Start(ctx context.Context) error {
	m.isStarted = true
	m.lifecycle = ctx
	return nil
}

func TestRunner_LifecycleCtxAliveAfterImmediateStart(t *testing.T) {
	starter := &immediateStarter{mockResource: mockResource{id: "http"}}
	r := &Runner{starters: []Starter{starter}}

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.Run(parent); err != nil {
		t.Fatal(err)
	}
	if !starter.isStarted {
		t.Fatal("starter not started")
	}
	if err := starter.lifecycle.Err(); err != nil {
		t.Fatalf("lifecycle ctx canceled after Run: %v", err)
	}

	cancel()
	if err := starter.lifecycle.Err(); err == nil {
		t.Fatal("expected lifecycle ctx canceled after parent shutdown")
	}
}

func TestRunner_FailFastCancelsBlockingStarter(t *testing.T) {
	errBoom := errors.New("boom")
	blocking := &activeResource{&mockResource{id: "blocking"}}
	failing := &activeResource{&mockResource{id: "fail", startErr: errBoom}}

	r := &Runner{starters: []Starter{blocking, failing}}

	done := make(chan struct{})
	go func() {
		_ = r.Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not finish after starter failure")
	}

	if !blocking.isStarted {
		t.Fatal("blocking starter was not started")
	}
}

func TestRunner_StopOrder(t *testing.T) {
	server := &activeResource{&mockResource{id: "server"}}
	db := &passiveResource{&mockResource{id: "db"}}

	r := &Runner{
		starters: []Starter{server},
		closers:  []Closer{db, server},
	}

	ctx := context.Background()

	go func() { _ = r.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)

	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if db.closedAt.Before(server.closedAt) {
		t.Error("expected server to be closed before db (reverse registration order)")
	}
}

func TestRunner_FailFast(t *testing.T) {
	errBoom := errors.New("boom")
	res1 := &activeResource{&mockResource{id: "ok"}}
	res2 := &activeResource{&mockResource{id: "fail", startErr: errBoom}}

	r := &Runner{starters: []Starter{res1, res2}}

	err := r.Run(context.Background())

	if !errors.Is(err, errBoom) {
		t.Errorf("expected error %v, got %v", errBoom, err)
	}
}

func TestRunner_StopPassive(t *testing.T) {
	db := &passiveResource{&mockResource{id: "db"}}
	r := &Runner{closers: []Closer{db}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunner_Inject(t *testing.T) {
	server := &activeResource{&mockResource{id: "server"}}
	r := &Runner{}
	r.Inject([]any{
		[]Starter{server},
		[]Closer{server},
	})
	if len(r.starters) != 1 || len(r.closers) != 1 {
		t.Fatalf("inject: starters=%d closers=%d", len(r.starters), len(r.closers))
	}
}
