package serve

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"mecha.im/internal/store"
	"mecha.im/internal/tasks"
	"mecha.im/internal/workers"
)

func TestScanPendingRespectsLongWorkerTimeout(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	reg, err := workers.NewRegistry(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Add(&workers.Worker{
		Name:     "long-worker",
		Endpoint: "http://127.0.0.1:1",
		Timeout:  28 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	taskStore := tasks.NewStore(db)
	server := New(Config{
		Registry: reg,
		Tasks:    taskStore,
		Addr:     "127.0.0.1:0",
	})

	ctx := context.Background()
	task, err := taskStore.Create(ctx, "long-worker", "long task")
	if err != nil {
		t.Fatal(err)
	}
	if err := taskStore.SetDispatched(ctx, task.ID); err != nil {
		t.Fatal(err)
	}

	// Twenty minutes is stale under the historical fixed 15-minute cutoff,
	// but is still active for a 28-minute worker plus five minutes of grace.
	_, err = db.ExecContext(
		ctx,
		`UPDATE tasks SET updated_at = ? WHERE id = ?`,
		time.Now().Add(-20*time.Minute).Unix(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	server.scanPending(ctx)
	select {
	case id := <-server.pending:
		t.Fatalf("active long task was re-enqueued: %s", id)
	default:
	}

	// Once worker timeout + grace has elapsed, crash recovery may re-enqueue.
	_, err = db.ExecContext(
		ctx,
		`UPDATE tasks SET updated_at = ? WHERE id = ?`,
		time.Now().Add(-34*time.Minute).Unix(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	server.scanPending(ctx)
	select {
	case id := <-server.pending:
		if id != task.ID {
			t.Fatalf("recovered id = %q, want %q", id, task.ID)
		}
	default:
		t.Fatal("truly stale long task was not re-enqueued")
	}
}
