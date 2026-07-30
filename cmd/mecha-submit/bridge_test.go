package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLock struct {
	err      error
	acquired int32
	released int32
}

func (f *fakeLock) Acquire(context.Context) (func() error, error) {
	if f.err != nil {
		return nil, f.err
	}
	atomic.AddInt32(&f.acquired, 1)
	return func() error {
		atomic.AddInt32(&f.released, 1)
		return nil
	}, nil
}

func testBridge(apiURL string) bridge {
	b := newBridge(apiURL, "test-api-key")
	b.pollInterval = time.Millisecond
	b.taskTimeout = 250 * time.Millisecond
	return b
}

func runTest(
	t *testing.T,
	b bridge,
	lock lockAcquirer,
	input string,
) (int, responseEnvelope) {
	t.Helper()
	var stdout bytes.Buffer
	code := run(
		context.Background(),
		strings.NewReader(input),
		&stdout,
		b,
		lock,
	)
	var resp responseEnvelope
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		t.Fatalf("decode stdout %q: %v", stdout.String(), err)
	}
	return code, resp
}

func TestRunCompletedAfterTransientPollFailure(t *testing.T) {
	lock := &fakeLock{}
	var gets int32

	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.Method == http.MethodPost && r.URL.Path == "/task" {
			if atomic.LoadInt32(&lock.acquired) != 1 {
				t.Error("POST occurred before global lock acquisition")
			}
			if got := r.Header.Get("Authorization"); got != "Bearer test-api-key" {
				t.Errorf("authorization = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode POST: %v", err)
			}
			if body["worker"] != "xiaoping-codex" {
				t.Errorf("worker = %q", body["worker"])
			}
			if body["prompt"] != "do the work" {
				t.Errorf("prompt = %q", body["prompt"])
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    "task-1",
				"state": "pending",
			})
			return
		}

		if r.Method == http.MethodGet && r.URL.Path == "/task/task-1" {
			call := atomic.AddInt32(&gets, 1)
			if call == 1 {
				http.Error(w, "temporary", http.StatusServiceUnavailable)
				return
			}
			if call == 2 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":    "task-1",
					"state": "dispatched",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":     "task-1",
				"state":  "completed",
				"result": `{"output":"done\n\"quoted\""}`,
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	code, resp := runTest(
		t,
		testBridge(srv.URL),
		lock,
		`{"request_id":"issue-12","prompt":"do the work"}`,
	)
	if code != 0 {
		t.Fatalf("code = %d, response = %+v", code, resp)
	}
	if resp.RequestID != "issue-12" ||
		resp.TaskID != "task-1" ||
		resp.State != "completed" ||
		resp.Output != "done\n\"quoted\"" ||
		resp.Error != "" {
		t.Errorf("unexpected response: %+v", resp)
	}
	if atomic.LoadInt32(&lock.released) != 1 {
		t.Errorf("lock release count = %d", lock.released)
	}
}

func TestInputValidationBeforeLock(t *testing.T) {
	oversized := `{"request_id":"r","prompt":"` +
		strings.Repeat("x", int(maxInputBytes)) +
		`"}`
	tests := []string{
		`{`,
		`{}`,
		`{"request_id":"r"}`,
		`{"request_id":"r","prompt":"   "}`,
		`{"request_id":"r","prompt":"x","worker":"other"}`,
		`{"request_id":"r","prompt":"x"} {"prompt":"second"}`,
		oversized,
	}

	for i, input := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			lock := &fakeLock{}
			code, resp := runTest(t, bridge{}, lock, input)
			if code != 1 || resp.State != "failed" || resp.Error == "" {
				t.Errorf("code=%d response=%+v", code, resp)
			}
			if atomic.LoadInt32(&lock.acquired) != 0 {
				t.Error("invalid input acquired global lock")
			}
		})
	}
}

func TestExactlyFortyKiBInputAccepted(t *testing.T) {
	prefix := `{"request_id":"r","prompt":"`
	suffix := `"}`
	promptLength := int(maxInputBytes) - len(prefix) - len(suffix)
	input := prefix + strings.Repeat("x", promptLength) + suffix
	if got := len(input); got != int(maxInputBytes) {
		t.Fatalf("input size = %d", got)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    "boundary",
				"state": "pending",
			})
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":     "boundary",
				"state":  "completed",
				"result": "plain result",
			})
		}
	}))
	defer srv.Close()

	code, resp := runTest(
		t,
		testBridge(srv.URL),
		&fakeLock{},
		input,
	)
	if code != 0 || resp.Output != "plain result" {
		t.Errorf("code=%d response=%+v", code, resp)
	}
}

func TestSubmitFailureRedactsSecret(t *testing.T) {
	secret := "sk-abcdefghijklmnopqrstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		http.Error(
			w,
			`{"error":"`+secret+`"}`,
			http.StatusServiceUnavailable,
		)
	}))
	defer srv.Close()

	lock := &fakeLock{}
	code, resp := runTest(
		t,
		testBridge(srv.URL),
		lock,
		`{"request_id":"r","prompt":"x"}`,
	)
	if code != 1 || !strings.Contains(resp.Error, "[REDACTED]") {
		t.Errorf("code=%d response=%+v", code, resp)
	}
	if strings.Contains(resp.Error, secret) {
		t.Errorf("secret leaked: %q", resp.Error)
	}
	if atomic.LoadInt32(&lock.released) != 1 {
		t.Error("lock not released after submit failure")
	}
}

func TestFailedTaskRedactsSecret(t *testing.T) {
	secret := "ghp_abcdefghijklmnopqrstuvwxyz"
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    "failed-task",
				"state": "pending",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":    "failed-task",
			"state": "failed",
			"error": "worker exposed " + secret,
		})
	}))
	defer srv.Close()

	code, resp := runTest(
		t,
		testBridge(srv.URL),
		&fakeLock{},
		`{"request_id":"r","prompt":"x"}`,
	)
	if code != 1 ||
		resp.State != "failed" ||
		!strings.Contains(resp.Error, "[REDACTED]") ||
		strings.Contains(resp.Error, secret) {
		t.Errorf("unexpected response: %+v", resp)
	}
}

func TestPermanentPollErrorIsNotRetried(t *testing.T) {
	var gets int32
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    "unauthorized",
				"state": "pending",
			})
			return
		}
		atomic.AddInt32(&gets, 1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	code, resp := runTest(
		t,
		testBridge(srv.URL),
		&fakeLock{},
		`{"request_id":"r","prompt":"x"}`,
	)
	if code != 1 || !strings.Contains(resp.Error, "401") {
		t.Errorf("code=%d response=%+v", code, resp)
	}
	if got := atomic.LoadInt32(&gets); got != 1 {
		t.Errorf("GET count = %d, want 1", got)
	}
}

func TestTaskTimeoutUsesInjectedDuration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":    "slow-task",
				"state": "pending",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":    "slow-task",
			"state": "pending",
		})
	}))
	defer srv.Close()

	b := testBridge(srv.URL)
	b.taskTimeout = 15 * time.Millisecond
	b.pollInterval = time.Millisecond

	code, resp := runTest(
		t,
		b,
		&fakeLock{},
		`{"request_id":"r","prompt":"x"}`,
	)
	if code != 1 ||
		resp.TaskID != "slow-task" ||
		resp.State != "pending" ||
		!strings.Contains(resp.Error, "timeout") {
		t.Errorf("code=%d response=%+v", code, resp)
	}
}

func TestLockFailurePreventsPOST(t *testing.T) {
	lock := &fakeLock{err: errors.New("lock unavailable")}
	code, resp := runTest(
		t,
		bridge{},
		lock,
		`{"request_id":"r","prompt":"x"}`,
	)
	if code != 1 ||
		resp.RequestID != "r" ||
		!strings.Contains(resp.Error, "lock unavailable") {
		t.Errorf("code=%d response=%+v", code, resp)
	}
}

func TestFileLockerTimeoutAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "global.lock")
	first := fileLocker{
		path:         path,
		wait:         50 * time.Millisecond,
		pollInterval: time.Millisecond,
	}
	release, err := first.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	second := fileLocker{
		path:         path,
		wait:         10 * time.Millisecond,
		pollInterval: time.Millisecond,
	}
	if _, err := second.Acquire(context.Background()); err == nil {
		t.Fatal("second acquire succeeded while lock held")
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	releaseAgain, err := second.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := releaseAgain(); err != nil {
		t.Fatalf("second release: %v", err)
	}
}
