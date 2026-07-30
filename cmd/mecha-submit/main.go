package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mecha.im/internal/workers"
)

const (
	productionAPIURL = "http://127.0.0.1:21212"
	productionLock   = "/run/lock/mecha-submit.lock"
	maxInputBytes    = int64(40 * 1024)
)

type inputEnvelope struct {
	RequestID string `json:"request_id"`
	Prompt    string `json:"prompt"`
}

type responseEnvelope struct {
	RequestID string `json:"request_id"`
	TaskID    string `json:"task_id"`
	State     string `json:"state"`
	Output    string `json:"output"`
	Error     string `json:"error"`
}

type lockAcquirer interface {
	Acquire(context.Context) (func() error, error)
}

type fileLocker struct {
	path         string
	wait         time.Duration
	pollInterval time.Duration
}

func main() {
	apiKey := os.Getenv("MECHA_API_KEY")
	if apiKey == "" {
		cfg, err := workers.LoadServerConfig()
		if err != nil {
			_ = writeResponse(os.Stdout, responseEnvelope{
				State: "failed",
				Error: safeError(fmt.Errorf("load mecha config: %w", err)),
			})
			os.Exit(1)
		}
		apiKey = cfg.APIKey
	}
	if apiKey == "" {
		_ = writeResponse(os.Stdout, responseEnvelope{
			State: "failed",
			Error: "mecha API authentication is not configured",
		})
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	b := newBridge(productionAPIURL, apiKey)
	lock := fileLocker{
		path:         productionLock,
		wait:         35 * time.Minute,
		pollInterval: time.Second,
	}
	code := run(ctx, os.Stdin, os.Stdout, b, lock)
	stop()
	os.Exit(code)
}

func run(
	ctx context.Context,
	stdin io.Reader,
	stdout io.Writer,
	b bridge,
	lock lockAcquirer,
) int {
	in, err := decodeInput(stdin)
	if err != nil {
		_ = writeResponse(stdout, responseEnvelope{
			State: "failed",
			Error: safeError(err),
		})
		return 1
	}

	release, err := lock.Acquire(ctx)
	if err != nil {
		_ = writeResponse(stdout, responseEnvelope{
			RequestID: in.RequestID,
			State:     "failed",
			Error:     safeError(err),
		})
		return 1
	}
	if release != nil {
		defer func() { _ = release() }()
	}

	resp, code := b.execute(ctx, in)
	if err := writeResponse(stdout, resp); err != nil {
		return 1
	}
	return code
}

func decodeInput(r io.Reader) (inputEnvelope, error) {
	raw, err := io.ReadAll(io.LimitReader(r, maxInputBytes+1))
	if err != nil {
		return inputEnvelope{}, fmt.Errorf("read input: %w", err)
	}
	if int64(len(raw)) > maxInputBytes {
		return inputEnvelope{}, fmt.Errorf(
			"input exceeds %d-byte limit",
			maxInputBytes,
		)
	}

	var in inputEnvelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return inputEnvelope{}, fmt.Errorf("decode input: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return inputEnvelope{}, fmt.Errorf("input must contain one JSON object")
	}
	if strings.TrimSpace(in.RequestID) == "" {
		return inputEnvelope{}, fmt.Errorf("request_id is required")
	}
	if strings.TrimSpace(in.Prompt) == "" {
		return inputEnvelope{}, fmt.Errorf("prompt is required")
	}
	return in, nil
}

func writeResponse(w io.Writer, resp responseEnvelope) error {
	return json.NewEncoder(w).Encode(resp)
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	return workers.RedactSecrets(strings.TrimSpace(err.Error()))
}

func (l fileLocker) Acquire(
	parent context.Context,
) (func() error, error) {
	wait := l.wait
	if wait <= 0 {
		wait = 35 * time.Minute
	}
	poll := l.pollInterval
	if poll <= 0 {
		poll = time.Second
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open global lock: %w", err)
	}

	ctx, cancel := context.WithTimeout(parent, wait)
	for {
		err = syscall.Flock(
			int(f.Fd()),
			syscall.LOCK_EX|syscall.LOCK_NB,
		)
		if err == nil {
			cancel()
			return func() error {
				unlockErr := syscall.Flock(
					int(f.Fd()),
					syscall.LOCK_UN,
				)
				closeErr := f.Close()
				if unlockErr != nil {
					return fmt.Errorf(
						"unlock global lock: %w",
						unlockErr,
					)
				}
				return closeErr
			}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			cancel()
			_ = f.Close()
			return nil, fmt.Errorf("acquire global lock: %w", err)
		}

		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			cancel()
			_ = f.Close()
			return nil, fmt.Errorf(
				"wait for global lock: %w",
				ctx.Err(),
			)
		case <-timer.C:
		}
	}
}
