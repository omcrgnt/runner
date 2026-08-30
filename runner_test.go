package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// starter is a fake Starter. When startErr is nil and noCleanup is false, it
// returns a cleanup that increments closeN (and, if set, appends id to
// cleanupOrder / sleeps cleanupDelay first). observeFlag/observedBefore let
// a test check what had already happened by the time Start ran.
type starter struct {
	id           string
	startErr     error
	cleanupErr   error
	noCleanup    bool
	cleanupOrder *[]string
	cleanupDelay time.Duration

	observeFlag    *bool
	observedBefore bool

	lifecycle  context.Context
	isStarted  bool
	closeN     atomic.Int32
	cleanupCtx context.Context
}

func (m *starter) Start(ctx context.Context) (func(context.Context) error, error) {
	if m.observeFlag != nil {
		m.observedBefore = *m.observeFlag
	}
	m.isStarted = true
	m.lifecycle = ctx
	if m.startErr != nil {
		return nil, m.startErr
	}
	if m.noCleanup {
		return nil, nil
	}
	return m.cleanup, nil
}

func (m *starter) cleanup(ctx context.Context) error {
	m.cleanupCtx = ctx
	if m.cleanupDelay > 0 {
		time.Sleep(m.cleanupDelay)
	}
	m.closeN.Add(1)
	if m.cleanupOrder != nil {
		*m.cleanupOrder = append(*m.cleanupOrder, m.id)
	}
	return m.cleanupErr
}

// lastStarter is a starter marked LastStarter: Run must not invoke its
// Start until every normal-wave Starter's Start has returned. waitFor and
// gate, if set, let a test observe what had already happened by the time
// this Start ran.
type lastStarter struct {
	starter
	waitFor          *starter
	gate             *fakeGate
	observedStarted  bool
	observedGateOpen bool
}

func (m *lastStarter) Start(ctx context.Context) (func(context.Context) error, error) {
	if m.waitFor != nil {
		m.observedStarted = m.waitFor.isStarted
	}
	if m.gate != nil {
		m.observedGateOpen = m.gate.opened
	}
	return m.starter.Start(ctx)
}

func (m *lastStarter) LastStart() {}

// standByResource is a fake StandBy. callOrder is appended to inside
// StandBy itself (sequential call order); cleanupOrder is appended to
// inside the returned cleanup (also sequential — Stop closes StandBys one
// at a time, unlike the concurrent Start-cleanup phase). doneFlag, if set,
// is flipped to true once StandBy succeeds, so a Starter's observeFlag can
// prove Start ran only after the whole StandBy phase finished.
type standByResource struct {
	id         string
	standByErr error
	cleanupErr error
	noCleanup  bool

	callOrder    *[]string
	cleanupOrder *[]string
	doneFlag     *bool

	standByN atomic.Int32
	closeN   atomic.Int32
}

func (m *standByResource) StandBy() (func(context.Context) error, error) {
	m.standByN.Add(1)
	if m.standByErr != nil {
		return nil, m.standByErr
	}
	if m.callOrder != nil {
		*m.callOrder = append(*m.callOrder, m.id)
	}
	if m.doneFlag != nil {
		*m.doneFlag = true
	}
	if m.noCleanup {
		return nil, nil
	}
	return m.cleanup, nil
}

func (m *standByResource) cleanup(context.Context) error {
	m.closeN.Add(1)
	if m.cleanupOrder != nil {
		*m.cleanupOrder = append(*m.cleanupOrder, m.id)
	}
	return m.cleanupErr
}

// passiveResource is a pure Closer: no Start, no StandBy, ready after Inject.
type passiveResource struct {
	id           string
	closeErr     error
	cleanupOrder *[]string
	closeN       atomic.Int32
}

func (m *passiveResource) Close(ctx context.Context) error {
	m.closeN.Add(1)
	if m.cleanupOrder != nil {
		*m.cleanupOrder = append(*m.cleanupOrder, m.id)
	}
	return m.closeErr
}

type fakeGate struct{ opened bool }

func (g *fakeGate) Open() { g.opened = true }

func TestRunner_LifecycleCtxAliveAfterImmediateStart(t *testing.T) {
	s := &starter{id: "http", noCleanup: true}
	r := &Runner{starters: []Starter{s}}

	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := r.Run(parent); err != nil {
		t.Fatal(err)
	}
	if !s.isStarted {
		t.Fatal("starter not started")
	}
	if err := s.lifecycle.Err(); err != nil {
		t.Fatalf("lifecycle ctx canceled after Run: %v", err)
	}

	cancel()
	if err := s.lifecycle.Err(); err == nil {
		t.Fatal("expected lifecycle ctx canceled after parent shutdown")
	}
}

func TestRunner_FailFastCancelsLifecycle(t *testing.T) {
	errBoom := errors.New("boom")
	ok := &starter{id: "ok", noCleanup: true}
	fail := &starter{id: "fail", startErr: errBoom}

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
	server := &starter{id: "server"}
	db := &passiveResource{id: "db"}

	r := &Runner{
		starters: []Starter{server},
		closers:  []Closer{db},
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

func TestRunner_FailFast(t *testing.T) {
	errBoom := errors.New("boom")
	res1 := &starter{id: "ok", noCleanup: true}
	res2 := &starter{id: "fail", startErr: errBoom}

	r := &Runner{starters: []Starter{res1, res2}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Errorf("expected error %v, got %v", errBoom, err)
	}
}

func TestRunner_StopPassive(t *testing.T) {
	db := &passiveResource{id: "db"}
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
	db := &passiveResource{id: "db"}
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

func TestRunner_Deps(t *testing.T) {
	deps := (&Runner{}).Deps()
	if len(deps) != 4 {
		t.Fatalf("Deps() len = %d, want 4", len(deps))
	}
}

func TestRunner_Inject(t *testing.T) {
	server := &starter{id: "server"}
	sb := &standByResource{id: "sb"}
	db := &passiveResource{id: "db"}
	gate := &fakeGate{}
	r := &Runner{}
	r.Inject([]any{
		[]Starter{server},
		[]StandBy{sb},
		[]Closer{db},
		gateOpener(gate),
	})
	if len(r.starters) != 1 || len(r.standBys) != 1 || len(r.closers) != 1 {
		t.Fatalf("inject: starters=%d standBys=%d closers=%d", len(r.starters), len(r.standBys), len(r.closers))
	}
	if r.gate == nil {
		t.Fatal("inject: gate not set")
	}
}

func TestRunner_LastStarterRunsAfterNormalWave(t *testing.T) {
	normal := &starter{id: "normal"}
	last := &lastStarter{starter: starter{id: "last"}, waitFor: normal}

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
	last := &lastStarter{starter: starter{id: "last"}, gate: gate}

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
	last := &lastStarter{starter: starter{id: "last", startErr: errBoom}}

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
	normal := &starter{id: "normal"}
	last := &lastStarter{starter: starter{id: "last", startErr: errBoom}}

	r := &Runner{starters: []Starter{normal, last}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if !normal.isStarted {
		t.Fatal("expected normal wave to have started before the last wave failed")
	}
}

func TestRunner_MultipleLastStarters(t *testing.T) {
	last1 := &lastStarter{starter: starter{id: "last1"}}
	last2 := &lastStarter{starter: starter{id: "last2"}}

	r := &Runner{starters: []Starter{last1, last2}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !last1.isStarted || !last2.isStarted {
		t.Fatal("expected both LastStarters to run")
	}
}

func TestRunner_StopClosesLastStarterToo(t *testing.T) {
	normal := &starter{id: "normal"}
	last := &lastStarter{starter: starter{id: "last"}}

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

func TestRunner_Stop_PassesCallerCtxToStartCleanup(t *testing.T) {
	s := &starter{id: "s"}

	r := &Runner{starters: []Starter{s}}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	if err := r.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if s.cleanupCtx == nil {
		t.Fatal("Starter cleanup was not called")
	}
	if _, ok := s.cleanupCtx.Deadline(); !ok {
		t.Fatal("Stop's caller ctx (with a deadline) was not passed through to the Starter cleanup — got a context with no deadline, want Stop's own ctx")
	}
	if s.cleanupCtx.Err() != nil {
		t.Fatalf("cleanup ctx already done: %v", s.cleanupCtx.Err())
	}
}

func TestRunner_Stop_StartCleanupsRunConcurrently(t *testing.T) {
	const delay = 100 * time.Millisecond
	a := &starter{id: "a", cleanupDelay: delay}
	b := &starter{id: "b", cleanupDelay: delay}
	c := &starter{id: "c", cleanupDelay: delay}

	r := &Runner{starters: []Starter{a, b, c}}
	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	// Sequential closing of 3 starters would take >=3*delay; a generous
	// bound well under that (but comfortably above a single delay) proves
	// they ran concurrently rather than one after another.
	if elapsed >= 3*delay {
		t.Fatalf("Stop took %v, want concurrent start-cleanups (well under %v)", elapsed, 3*delay)
	}
	for _, s := range []*starter{a, b, c} {
		if s.closeN.Load() != 1 {
			t.Fatalf("%s close count = %d, want 1", s.id, s.closeN.Load())
		}
	}
}

func TestRunner_Stop_StandByAndCloserCleanupsReverseOrder(t *testing.T) {
	var sbOrder, closerOrder []string

	sb1 := &standByResource{id: "sb1", cleanupOrder: &sbOrder}
	sb2 := &standByResource{id: "sb2", cleanupOrder: &sbOrder}
	sb3 := &standByResource{id: "sb3", cleanupOrder: &sbOrder}

	db1 := &passiveResource{id: "db1", cleanupOrder: &closerOrder}
	db2 := &passiveResource{id: "db2", cleanupOrder: &closerOrder}

	r := &Runner{
		standBys: []StandBy{sb1, sb2, sb3},
		closers:  []Closer{db1, db2},
	}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	wantSB := []string{"sb3", "sb2", "sb1"}
	if len(sbOrder) != len(wantSB) {
		t.Fatalf("standby cleanup order = %v, want %v", sbOrder, wantSB)
	}
	for i := range wantSB {
		if sbOrder[i] != wantSB[i] {
			t.Fatalf("standby cleanup order = %v, want %v", sbOrder, wantSB)
		}
	}

	wantCloser := []string{"db2", "db1"}
	if len(closerOrder) != len(wantCloser) {
		t.Fatalf("closer order = %v, want %v", closerOrder, wantCloser)
	}
	for i := range wantCloser {
		if closerOrder[i] != wantCloser[i] {
			t.Fatalf("closer order = %v, want %v", closerOrder, wantCloser)
		}
	}
}

func TestRunner_StandByRunsInRegistrationOrder(t *testing.T) {
	var order []string
	sb1 := &standByResource{id: "first", callOrder: &order}
	sb2 := &standByResource{id: "second", callOrder: &order}
	sb3 := &standByResource{id: "third", callOrder: &order}

	r := &Runner{standBys: []StandBy{sb1, sb2, sb3}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := []string{"first", "second", "third"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestRunner_StandByRunsBeforeStartPhase(t *testing.T) {
	var standByDone bool
	sb := &standByResource{id: "sb", doneFlag: &standByDone}
	s := &starter{id: "s", observeFlag: &standByDone}

	r := &Runner{standBys: []StandBy{sb}, starters: []Starter{s}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !s.observedBefore {
		t.Fatal("Start ran before the StandBy phase had finished")
	}
}

func TestRunner_StandByFailure_AbortsBeforeStartPhase(t *testing.T) {
	errBoom := errors.New("standby: boom")
	sb1 := &standByResource{id: "sb1"}
	sb2 := &standByResource{id: "sb2", standByErr: errBoom}
	sb3 := &standByResource{id: "sb3"}
	s := &starter{id: "s"}

	r := &Runner{standBys: []StandBy{sb1, sb2, sb3}, starters: []Starter{s}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if sb1.closeN.Load() != 1 {
		t.Fatal("earlier-succeeded StandBy was not rolled back")
	}
	if sb3.standByN.Load() != 0 {
		t.Fatal("StandBy registered after the failure must never be called")
	}
	if s.isStarted {
		t.Fatal("Start phase must not run when the StandBy phase fails")
	}
}

func TestRunner_StandByFailure_GateNeverOpens(t *testing.T) {
	errBoom := errors.New("boom")
	gate := &fakeGate{}
	sb := &standByResource{id: "sb", standByErr: errBoom}

	r := &Runner{standBys: []StandBy{sb}, gate: gate}

	if err := r.Run(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if gate.opened {
		t.Fatal("Gate must not open when the StandBy phase fails")
	}
}

func TestRunner_StartFailure_RollsBackStandByAndStartedStarters(t *testing.T) {
	errBoom := errors.New("start: boom")
	sb := &standByResource{id: "sb"}
	ok := &starter{id: "ok"}
	fail := &starter{id: "fail", startErr: errBoom}

	r := &Runner{standBys: []StandBy{sb}, starters: []Starter{ok, fail}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if sb.closeN.Load() != 1 {
		t.Fatal("StandBy cleanup was not rolled back after a Start failure")
	}
	if ok.closeN.Load() != 1 {
		t.Fatal("already-started Starter's cleanup was not rolled back after a sibling Start failure")
	}

	// A later Stop (e.g. app.Serve always calls Stop after Run, regardless
	// of its error) must not invoke either cleanup a second time.
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sb.closeN.Load() != 1 {
		t.Fatal("StandBy cleanup was invoked again by a later Stop")
	}
	if ok.closeN.Load() != 1 {
		t.Fatal("Starter cleanup was invoked again by a later Stop")
	}
}

func TestRunner_StartFailure_PartialWaveOnlyRollsBackStartedOnes(t *testing.T) {
	errBoom := errors.New("boom")
	ok := &starter{id: "ok"}
	fail := &starter{id: "fail", startErr: errBoom}

	r := &Runner{starters: []Starter{ok, fail}}

	err := r.Run(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run err = %v, want %v", err, errBoom)
	}
	if ok.closeN.Load() != 1 {
		t.Fatalf("ok close count = %d, want 1", ok.closeN.Load())
	}
	// fail's Start never succeeded, so it has no cleanup to have run.
}

func TestRunner_StopIsIdempotentAfterSuccessfulRun(t *testing.T) {
	sb := &standByResource{id: "sb"}
	s := &starter{id: "s"}
	db := &passiveResource{id: "db"}

	r := &Runner{standBys: []StandBy{sb}, starters: []Starter{s}, closers: []Closer{db}}

	if err := r.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if sb.closeN.Load() != 1 {
		t.Fatalf("sb close count = %d, want 1", sb.closeN.Load())
	}
	if s.closeN.Load() != 1 {
		t.Fatalf("s close count = %d, want 1", s.closeN.Load())
	}
	// db is a pure Closer, not a one-shot cleanup closure: a second Stop
	// calling Close again is expected/unchanged behavior, not double-close
	// protection territory (Runner has no record of "already closed" for
	// closers, same as before this change).
	if db.closeN.Load() != 2 {
		t.Fatalf("db close count = %d, want 2 (pure Closer.Close has no once-guard)", db.closeN.Load())
	}
}
