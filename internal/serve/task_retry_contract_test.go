package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"mecha.im/internal/store"
	"mecha.im/internal/tasks"
	"mecha.im/internal/workers"
)

func TestDirectTaskAcceptsExplicitRetryCeiling(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	reg, err := workers.NewRegistry(db)
	if err != nil {
		t.Fatal(err)
	}
	taskStore := tasks.NewStore(db)
	server := New(Config{
		Registry: reg,
		Tasks:    taskStore,
		Addr:     "127.0.0.1:0",
	})
	httpServer := httptest.NewServer(server.httpSrv.Handler)
	defer httpServer.Close()

	body := bytes.NewBufferString(
		`{"prompt":"one attempt","worker":"xiaoping-codex","max_retries":1}`,
	)
	response, err := http.Post(httpServer.URL+"/task", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", response.StatusCode)
	}

	var created tasks.Task
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.MaxRetries != 1 {
		t.Fatalf("response max_retries = %d, want 1", created.MaxRetries)
	}
	stored, err := taskStore.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.MaxRetries != 1 {
		t.Fatalf("stored max_retries = %d, want 1", stored.MaxRetries)
	}
}

func TestDirectTaskRejectsInvalidRetryCeiling(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	reg, err := workers.NewRegistry(db)
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{
		Registry: reg,
		Tasks:    tasks.NewStore(db),
		Addr:     "127.0.0.1:0",
	})
	httpServer := httptest.NewServer(server.httpSrv.Handler)
	defer httpServer.Close()

	body := bytes.NewBufferString(
		`{"prompt":"invalid","worker":"xiaoping-codex","max_retries":0}`,
	)
	response, err := http.Post(httpServer.URL+"/task", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
}
