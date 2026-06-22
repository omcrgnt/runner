package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omcrgnt/res/restest"
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

func TestRunner_StopOrder(t *testing.T) {
	server := &activeResource{&mockResource{id: "server"}}
	db := &passiveResource{&mockResource{id: "db"}}

	r := New(restest.With(db, server))

	ctx := context.Background()

	go r.run(ctx)
	time.Sleep(10 * time.Millisecond)

	r.stop(ctx)

	if db.closedAt.Before(server.closedAt) {
		t.Error("expected server (active) to be closed before db (passive)")
	}
}

func TestRunner_FailFast(t *testing.T) {
	errBoom := errors.New("boom")
	res1 := &activeResource{&mockResource{id: "ok"}}
	res2 := &activeResource{&mockResource{id: "fail", startErr: errBoom}}

	r := New(restest.With(res1, res2))

	err := r.run(context.Background())

	if !errors.Is(err, errBoom) {
		t.Errorf("expected error %v, got %v", errBoom, err)
	}
}

func TestRunner_NewResource(t *testing.T) {
	var r Runner
	got, err := r.NewResource()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(*Runner); !ok {
		t.Fatalf("expected *Runner, got %T", got)
	}
}

func TestRunner_RunStopViaRegistry(t *testing.T) {
	reg := restest.With(&passiveResource{&mockResource{id: "db"}})
	var r Runner
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Stop(ctx, reg); err != nil {
		t.Fatal(err)
	}
}
