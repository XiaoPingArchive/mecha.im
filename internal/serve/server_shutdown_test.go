package serve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mecha.im/internal/store"
	"mecha.im/internal/tasks"
	"mecha.im/internal/workers"
)

func startShutdownTestServer(
	t *testing.T,
	workerHandler http.Handler,
	drainTimeout time.Duration,
) (*Server, *tasks.Store, context.CancelFunc, <-chan error, func()) {
	t.Helper()

	db, err := store.Open(filepath.Join(t.TempDir(), "shutdown.db"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := workers.NewRegistry(db)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	taskStore := tasks.NewStore(db)
	workerServer := httptest.NewServer(workerHandler)
	if err := reg.Add(&workers.Worker{Name: "slow", Endpoint: workerServer.URL, Timeout: time.Minute}); err != nil {
		workerServer.Close()
		db.Close()
		t.Fatal(err)
	}
	if err := reg.Start("slow"); err != nil {
		workerServer.Close()
		db.Close()
		t.Fatal(err)
	}

	srv := New(Config{
		Registry:     reg,
		Tasks:        taskStore,
		Addr:         "127.0.0.1:0",
		DrainTimeout: drainTimeout,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errCh <- srv.Start(ctx)
	}()
	select {
	case <-srv.ready:
	case err := <-errCh:
		workerServer.CloseClientConnections()
		workerServer.Close()
		db.Close()
		t.Fatalf("server failed before listening: %v", err)
	case <-time.After(2 * time.Second):
		cancel()
		workerServer.CloseClientConnections()
		workerServer.Close()
		db.Close()
		t.Fatal("server did not start listening")
	}

	cleanup := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop during cleanup")
		}
		workerServer.CloseClientConnections()
		workerServer.Close()
		db.Close()
	}
	return srv, taskStore, cancel, errCh, cleanup
}

func postShutdownTestTask(t *testing.T, srv *Server) string {
	t.Helper()
	req := httptest.NewRequest(
		http.MethodPost,
		"/task",
		strings.NewReader(`{"prompt":"wait","worker":"slow","max_retries":1}`),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("post status = %d: %s", rec.Code, rec.Body.String())
	}
	var task tasks.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	return task.ID
}

func waitUntilDraining(t *testing.T, srv *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !srv.draining.Load() {
		if time.Now().After(deadline) {
			t.Fatal("server did not enter draining state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestShutdownDrainsActiveDispatchAndRejectsNewTasks(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	requestCancelled := make(chan struct{})
	var enteredOnce sync.Once
	var cancelledOnce sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"output":"drained"}`))
		case <-r.Context().Done():
			cancelledOnce.Do(func() { close(requestCancelled) })
		}
	})

	srv, taskStore, cancel, errCh, cleanup := startShutdownTestServer(t, handler, time.Second)
	defer cleanup()
	taskID := postShutdownTestTask(t, srv)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker request did not start")
	}

	cancel()
	waitUntilDraining(t, srv)

	rejected := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/task",
		strings.NewReader(`{"prompt":"new","worker":"slow"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	srv.httpSrv.Handler.ServeHTTP(rejected, req)
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("post while draining = %d, want 503", rejected.Code)
	}

	poll := httptest.NewRecorder()
	srv.httpSrv.Handler.ServeHTTP(
		poll,
		httptest.NewRequest(http.MethodGet, "/task/"+taskID, nil),
	)
	if poll.Code != http.StatusOK {
		t.Fatalf("poll while draining = %d, want 200", poll.Code)
	}

	select {
	case <-requestCancelled:
		t.Fatal("active request was cancelled before drain deadline")
	case err := <-errCh:
		t.Fatalf("server returned before active task completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after active task completed")
	}

	got, err := taskStore.Get(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != tasks.StateCompleted {
		t.Fatalf("task state = %q, want completed", got.State)
	}
}

func TestShutdownCancelsAtDeadlineAndPersistsFailure(t *testing.T) {
	entered := make(chan struct{})
	requestCancelled := make(chan struct{})
	var enteredOnce sync.Once
	var cancelledOnce sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enteredOnce.Do(func() { close(entered) })
		<-r.Context().Done()
		cancelledOnce.Do(func() { close(requestCancelled) })
	})

	const drain = 250 * time.Millisecond
	srv, taskStore, cancel, errCh, cleanup := startShutdownTestServer(t, handler, drain)
	defer cleanup()
	taskID := postShutdownTestTask(t, srv)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker request did not start")
	}

	startedDrain := time.Now()
	cancel()
	waitUntilDraining(t, srv)
	select {
	case <-requestCancelled:
		t.Fatal("request cancelled before drain deadline")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-requestCancelled:
		if elapsed := time.Since(startedDrain); elapsed < 200*time.Millisecond {
			t.Fatalf("request cancelled too early after %s", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request was not cancelled after drain deadline")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after forced cancellation")
	}

	got, err := taskStore.Get(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != tasks.StateFailed {
		t.Fatalf("task state = %q, want failed", got.State)
	}
	if !strings.Contains(got.ErrorMsg, "task cancelled") {
		t.Fatalf("task error = %q, want cancellation reason", got.ErrorMsg)
	}
}
