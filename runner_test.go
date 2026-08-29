package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type mockResource struct {
	id        string
	startErr  error
	closeErr  error
	isStarted bool
	closeN    atomic.Int32
}

type passiveResource struct{ *mockResource }

func (m *passiveResource) Close(ctx context.Context) error {
	m.closeN.Add(1)
	return m.closeErr
}

// immediateStarter returns from Start without blocking (like srv-http).
type immediateStarter struct {
	mockResource
	lifecycle context.Context
}

func (m *immediateStarter) Start(ctx context.Context) error {
	m.isStarted = true
	m.lifecycle = ctx
	if m.startErr != nil {
		return m.startErr
	}
	return nil
}

// immediateLifecycle is Starter+Closer that returns from Start immediately.
type immediateLifecycle struct {
	mockResource
}

func (m *immediateLifecycle) Start(ctx context.Context) error {
	m.isStarted = true
	if m.startErr != nil {
		return m.startErr
	}
	return nil
}

func (m *immediateLifecycle) Close(ctx context.Context) error {
	m.closeN.Add(1)
	return m.closeErr
}

// lastImmediateStarter is a Starter marked LastStarter: Run must not invoke
// its Start until every normal-wave Starter's Start has returned. waitFor
// and gate, if set, let a test observe what had already happened by the
// time this Start ran.
type lastImmediateStarter struct {
	mockResource
	waitFor         *mockResource
	gate            *fakeGate
	observedStarted bool
	observedGateOpen bool
}

func (m *lastImmediateStarter) Start(ctx context.Context) error {
	m.isStarted = true
	if m.waitFor != nil {
		m.observedStarted = m.waitFor.isStarted
	}
	if m.gate != nil {
		m.observedGateOpen = m.gate.opened
	}
	if m.startErr != nil {
		return m.startErr
	}
	return nil
}

func (m *lastImmediateStarter) LastStart() {}

// lastImmediateLifecycle is used only by TestRunner_StopClosesLastStarterToo,
// to prove Stop's existing Closer-closing loop needs no changes to also
// cover the last wave (it iterates r.starters by original index either way).
type lastImmediateLifecycle struct {
	mockResource
}

func (m *lastImmediateLifecycle) Start(ctx context.Context) error {
	m.isStarted = true
	if m.startErr != nil {
		return m.startErr
	}
	return nil
}

func (m *lastImmediateLifecycle) Close(ctx context.Context) error {
	m.closeN.Add(1)
	return m.closeErr
}

func (m *lastImmediateLifecycle) LastStart() {}

type fakeGate struct{ opened bool }

func (g *fakeGate) Open() { g.opened = true }

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

func TestRunner_FailFastCancelsLifecycle(t *testing.T) {
	errBoom := errors.New("boom")
	ok := &immediateStarter{mockResource: mockResource{id: "ok"}}
	fail := &immediateStarter{mockResource: mockResource{id: "fail", startErr: errBoom}}

	r := &Runner{starters: []Starter{ok, fail}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if ok.lifecycle == nil {
		t.Fatal("expected ok starter to receive lifecycle ctx")
	}
	if err := ok.lifecycle.Err(); err == nil {
		t.Fatal("expected lifecycle canceled after fail-fast")
	}
}

func TestRunner_StopClosesStartedAndPureClosers(t *testing.T) {
	server := &immediateLifecycle{mockResource: mockResource{id: "server"}}
	db := &passiveResource{&mockResource{id: "db"}}

	r := &Runner{
		starters: []Starter{server},
		closers:  []Closer{db, server},
	}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if server.closeN.Load() != 1 {
		t.Fatalf("server close count = %d, want 1", server.closeN.Load())
	}
	if db.closeN.Load() != 1 {
		t.Fatalf("db close count = %d, want 1", db.closeN.Load())
	}
}

func TestRunner_FailFast_PartialClose(t *testing.T) {
	errBoom := errors.New("boom")
	ok := &immediateLifecycle{mockResource: mockResource{id: "ok"}}
	fail := &immediateLifecycle{mockResource: mockResource{id: "fail", startErr: errBoom}}

	r := &Runner{
		starters: []Starter{ok, fail},
		closers:  []Closer{ok, fail},
	}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}

	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if ok.closeN.Load() != 1 {
		t.Fatalf("ok close count = %d, want 1", ok.closeN.Load())
	}
	if fail.closeN.Load() != 0 {
		t.Fatalf("fail close count = %d, want 0", fail.closeN.Load())
	}
}

func TestRunner_FailFast(t *testing.T) {
	errBoom := errors.New("boom")
	res1 := &immediateStarter{mockResource: mockResource{id: "ok"}}
	res2 := &immediateStarter{mockResource: mockResource{id: "fail", startErr: errBoom}}

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
	if db.closeN.Load() != 1 {
		t.Fatalf("db close count = %d, want 1", db.closeN.Load())
	}
}

func TestRunner_StopPureCloser_NoStarters(t *testing.T) {
	db := &passiveResource{&mockResource{id: "db"}}
	r := &Runner{closers: []Closer{db}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.closeN.Load() != 1 {
		t.Fatalf("db close count = %d, want 1", db.closeN.Load())
	}
}

func TestRunner_Inject(t *testing.T) {
	server := &immediateLifecycle{mockResource: mockResource{id: "server"}}
	gate := &fakeGate{}
	r := &Runner{}
	r.Inject([]any{
		[]Starter{server},
		[]Closer{server},
		gateOpener(gate),
	})
	if len(r.starters) != 1 || len(r.closers) != 1 {
		t.Fatalf("inject: starters=%d closers=%d", len(r.starters), len(r.closers))
	}
	if r.gate == nil {
		t.Fatal("inject: gate not set")
	}
}

func TestRunner_LastStarterRunsAfterNormalWave(t *testing.T) {
	normal := &immediateLifecycle{mockResource: mockResource{id: "normal"}}
	last := &lastImmediateStarter{mockResource: mockResource{id: "last"}, waitFor: &normal.mockResource}

	// Deliberately registered before normal: ordering must come from the
	// LastStart marker, not from registration position.
	r := &Runner{starters: []Starter{last, normal}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !last.observedStarted {
		t.Fatal("LastStarter ran before the normal wave finished")
	}
}

func TestRunner_GateOpensOnlyAfterLastWave(t *testing.T) {
	gate := &fakeGate{}
	last := &lastImmediateStarter{mockResource: mockResource{id: "last"}, gate: gate}

	r := &Runner{starters: []Starter{last}, gate: gate}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if last.observedGateOpen {
		t.Fatal("LastStarter observed Gate already open during its own Start")
	}
	if !gate.opened {
		t.Fatal("expected Gate open after Run returns")
	}
}

func TestRunner_LastWaveFailure_GateNeverOpens(t *testing.T) {
	errBoom := errors.New("boom")
	gate := &fakeGate{}
	last := &lastImmediateStarter{mockResource: mockResource{id: "last", startErr: errBoom}}

	r := &Runner{starters: []Starter{last}, gate: gate}

	if err := r.Run(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if gate.opened {
		t.Fatal("Gate must not open when the last wave fails — Ready() would lie about readiness")
	}
}

func TestRunner_LastWaveFailFast(t *testing.T) {
	errBoom := errors.New("boom")
	normal := &immediateLifecycle{mockResource: mockResource{id: "normal"}}
	last := &lastImmediateStarter{mockResource: mockResource{id: "last", startErr: errBoom}}

	r := &Runner{starters: []Starter{normal, last}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if !normal.isStarted {
		t.Fatal("expected normal wave to have started before the last wave failed")
	}
}

func TestRunner_Deps(t *testing.T) {
	deps := (&Runner{}).Deps()
	if len(deps) != 3 {
		t.Fatalf("Deps() len = %d, want 3", len(deps))
	}
}

func TestRunner_MultipleLastStarters(t *testing.T) {
	last1 := &lastImmediateStarter{mockResource: mockResource{id: "last1"}}
	last2 := &lastImmediateStarter{mockResource: mockResource{id: "last2"}}

	r := &Runner{starters: []Starter{last1, last2}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !last1.isStarted || !last2.isStarted {
		t.Fatal("expected both LastStarters to run")
	}
}

func TestRunner_StopClosesLastStarterToo(t *testing.T) {
	normal := &immediateLifecycle{mockResource: mockResource{id: "normal"}}
	last := &lastImmediateLifecycle{mockResource: mockResource{id: "last"}}

	r := &Runner{starters: []Starter{normal, last}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if normal.closeN.Load() != 1 {
		t.Fatalf("normal close count = %d, want 1", normal.closeN.Load())
	}
	if last.closeN.Load() != 1 {
		t.Fatalf("last close count = %d, want 1", last.closeN.Load())
	}
}
