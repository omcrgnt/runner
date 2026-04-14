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
	<-ctx.Done() // Имитируем работу до отмены контекста
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
	// Создаем ресурсы: один активный (сервер), один пассивный (бд)
	server := &activeResource{&mockResource{id: "server"}}
	db := &passiveResource{&mockResource{id: "db"}}

	// Порядок в списке: сначала бд, потом сервер (как после DAG)
	r := New([]any{db, server})

	ctx := context.Background()

	// Запускаем сервер
	go r.Run(ctx)
	time.Sleep(10 * time.Millisecond) // Даем время стартовать

	// Останавливаем
	r.Stop(ctx)

	if db.closedAt.Before(server.closedAt) {
		t.Error("expected server (active) to be closed before db (passive)")
	}
}

func TestRunner_FailFast(t *testing.T) {
	errBoom := errors.New("boom")
	res1 := &activeResource{&mockResource{id: "ok"}}
	res2 := &activeResource{&mockResource{id: "fail", startErr: errBoom}}

	r := New([]any{res1, res2})

	err := r.Run(context.Background())

	if !errors.Is(err, errBoom) {
		t.Errorf("expected error %v, got %v", errBoom, err)
	}
}
