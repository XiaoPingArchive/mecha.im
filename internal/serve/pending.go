package serve

import (
	"context"
	"time"

	"mecha.im/internal/tasks"
)

const (
	pendingScanInterval = 60 * time.Second
	defaultTaskTimeout  = 10 * time.Minute
	staleDispatchGrace  = 5 * time.Minute
)

// staleDispatchAfter keeps recovery behind the configured worker timeout.
// A fixed threshold can duplicate long-running disposable tasks.
func (s *Server) staleDispatchAfter(t *tasks.Task) time.Duration {
	timeout := defaultTaskTimeout
	if entry, ok := s.reg.Get(t.WorkerName); ok && entry.Worker != nil && entry.Worker.Timeout > 0 {
		timeout = entry.Worker.Timeout
	}
	return timeout + staleDispatchGrace
}

// pendingLoop scans for orphaned pending tasks (created but never dispatched
// — e.g., if the channel was full when they were enqueued). Catches tasks
// that slipped through the in-memory queue.
func (s *Server) pendingLoop(ctx context.Context) {
	ticker := time.NewTicker(pendingScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("pending loop stopped")
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error("pending loop: panic", "panic", r)
					}
				}()
				s.scanPending(ctx)
			}()
		}
	}
}

func (s *Server) scanPending(ctx context.Context) {
	ids, err := s.tasks.Pending(ctx)
	if err != nil {
		s.logger.Error("pending: scan failed", "err", err)
		return
	}
	for _, id := range ids {
		t, err := s.tasks.Get(ctx, id)
		if err != nil {
			s.logger.Warn("pending: get task", "id", id, "err", err)
			continue
		}
		// Skip tasks with future retry times
		if t.NextRetryAt != nil && t.NextRetryAt.After(time.Now()) {
			continue
		}
		// Re-enqueue dispatched tasks only after their own worker timeout plus
		// a recovery grace period. This avoids duplicating an active long task.
		if t.State == tasks.StateDispatched {
			age := time.Since(t.UpdatedAt)
			staleAfter := s.staleDispatchAfter(t)
			if age < staleAfter {
				continue
			}
			s.logger.Warn("pending: stale dispatched task, re-enqueuing", "id", id, "age", age.Truncate(time.Second), "stale_after", staleAfter)
		}
		// Dedup check: don't re-dispatch if already completed
		if t.DedupKey != "" {
			dup, err := s.tasks.HasCompletedDedup(ctx, t.DedupKey)
			if err != nil {
				s.logger.Warn("pending: dedup check", "id", id, "err", err)
				continue
			}
			if dup {
				if failErr := s.tasks.Fail(ctx, id, "skipped: duplicate of completed task"); failErr != nil {
					s.logger.Warn("pending: fail dedup task", "id", id, "err", failErr)
				}
				continue
			}
		}
		select {
		case s.pending <- id:
			s.logger.Info("pending: recovered orphan", "id", id)
		default:
			s.logger.Warn("pending: queue full, skipping orphan", "id", id)
		}
	}
}
