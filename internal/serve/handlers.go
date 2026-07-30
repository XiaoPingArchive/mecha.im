package serve

import (
	"errors"
	"net/http"
	"sync/atomic"

	"mecha.im/internal/tasks"
	"mecha.im/internal/workers"
)

type taskRequest struct {
	Prompt     string `json:"prompt"`
	Worker     string `json:"worker"`
	MaxRetries *int   `json:"max_retries,omitempty"`
}

var workerRoundRobin atomic.Uint64

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	qLen := len(s.pending)
	qCap := cap(s.pending)
	resp := map[string]any{
		"status":      "ok",
		"queue_depth": qLen,
		"queue_cap":   qCap,
	}
	if float64(qLen) > float64(qCap)*0.9 {
		resp["status"] = "degraded"
		resp["reason"] = "pending queue near capacity"
		writeJSON(w, http.StatusServiceUnavailable, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePostTask(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeError(w, http.StatusServiceUnavailable, "server draining")
		return
	}

	var req taskRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Prompt == "" {
		writeError(w, http.StatusBadRequest, "prompt is required")
		return
	}
	if req.MaxRetries != nil && (*req.MaxRetries < 1 || *req.MaxRetries > 10) {
		writeError(w, http.StatusBadRequest, "max_retries must be between 1 and 10")
		return
	}
	if req.Worker == "" {
		entries := s.reg.List()
		var online []string
		for _, e := range entries {
			if e.State == workers.StateOnline {
				online = append(online, e.Worker.Name)
			}
		}
		if len(online) == 0 {
			writeError(w, http.StatusServiceUnavailable, "no online workers")
			return
		}
		idx := workerRoundRobin.Add(1)
		req.Worker = online[int(idx-1)%len(online)]
	}

	var t *tasks.Task
	var err error
	if req.MaxRetries == nil {
		t, err = s.tasks.Create(r.Context(), req.Worker, req.Prompt)
	} else {
		t, err = s.tasks.CreateWithMaxRetries(r.Context(), req.Worker, req.Prompt, *req.MaxRetries)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create task")
		return
	}
	tasksCreated.Add(1)

	// Close the race where shutdown begins while the request is being parsed or
	// persisted. The task is made terminal instead of being stranded in memory.
	if s.draining.Load() {
		if err := s.tasks.Fail(r.Context(), t.ID, "server draining"); err != nil {
			s.logger.Error("fail task during drain", "id", t.ID, "err", err)
		}
		writeError(w, http.StatusServiceUnavailable, "server draining")
		return
	}

	select {
	case s.pending <- t.ID:
		queueDepth.Add(1)
		writeJSON(w, http.StatusAccepted, t)
	default:
		if err := s.tasks.Fail(r.Context(), t.ID, "task queue full"); err != nil {
			s.logger.Error("fail task on queue full", "id", t.ID, "err", err)
		}
		writeError(w, http.StatusTooManyRequests, "task queue full")
	}
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}
	t, err := s.tasks.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, tasks.ErrNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
		} else {
			writeError(w, http.StatusInternalServerError, "internal error")
		}
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	tasks, err := s.tasks.List(r.Context(), state)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if tasks == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	entries := s.reg.List()
	sanitized := make([]workers.Entry, len(entries))
	for i := range entries {
		sanitized[i] = entries[i].Sanitized()
	}
	writeJSON(w, http.StatusOK, sanitized)
}
