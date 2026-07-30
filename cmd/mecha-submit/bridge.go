package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"mecha.im/internal/tasks"
)

const maxHTTPBodyBytes = int64(10 << 20)

type bridge struct {
	apiURL          string
	apiKey          string
	worker          string
	client          *http.Client
	pollInterval    time.Duration
	taskTimeout     time.Duration
	maxResponseSize int64
}

func newBridge(apiURL, apiKey string) bridge {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return bridge{
		apiURL: strings.TrimRight(apiURL, "/"),
		apiKey: apiKey,
		worker: "xiaoping-codex",
		client: &http.Client{
			Transport: transport,
			Timeout:   15 * time.Second,
			CheckRedirect: func(
				_ *http.Request,
				_ []*http.Request,
			) error {
				return http.ErrUseLastResponse
			},
		},
		pollInterval:    2 * time.Second,
		taskTimeout:     30 * time.Minute,
		maxResponseSize: maxHTTPBodyBytes,
	}
}

func (b bridge) execute(
	parent context.Context,
	in inputEnvelope,
) (responseEnvelope, int) {
	resp := responseEnvelope{
		RequestID: in.RequestID,
		State:     "failed",
	}
	ctx, cancel := context.WithTimeout(parent, b.taskTimeout)
	defer cancel()

	task, err := b.submit(ctx, in.Prompt)
	if err != nil {
		resp.Error = safeError(err)
		return resp, 1
	}
	resp.TaskID = task.ID
	lastState := task.State
	if lastState == "" {
		lastState = tasks.StatePending
	}

	poll := b.pollInterval
	if poll <= 0 {
		poll = 2 * time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		current, retryable, err := b.getTask(ctx, task.ID)
		if err == nil {
			if current.State != "" {
				lastState = current.State
			}
			switch current.State {
			case tasks.StateCompleted:
				resp.State = string(tasks.StateCompleted)
				resp.Output = extractOutput(current.Result)
				return resp, 0
			case tasks.StateFailed:
				resp.State = string(tasks.StateFailed)
				if current.ErrorMsg == "" {
					current.ErrorMsg = "mecha task failed"
				}
				resp.Error = safeError(errors.New(current.ErrorMsg))
				return resp, 1
			case tasks.StatePending, tasks.StateDispatched:
			default:
				resp.State = "failed"
				resp.Error = safeError(fmt.Errorf(
					"task returned unknown state %q",
					current.State,
				))
				return resp, 1
			}
		} else if !retryable {
			resp.State = "failed"
			resp.Error = safeError(err)
			return resp, 1
		}

		select {
		case <-ctx.Done():
			resp.State = string(lastState)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				resp.Error = safeError(fmt.Errorf(
					"timeout after %s waiting for task",
					b.taskTimeout,
				))
			} else {
				resp.Error = safeError(fmt.Errorf(
					"task wait cancelled: %w",
					ctx.Err(),
				))
			}
			return resp, 1
		case <-ticker.C:
		}
	}
}

func (b bridge) submit(
	ctx context.Context,
	prompt string,
) (tasks.Task, error) {
	payload := struct {
		Prompt     string `json:"prompt"`
		Worker     string `json:"worker"`
		MaxRetries int    `json:"max_retries"`
	}{
		Prompt:     prompt,
		Worker:     b.worker,
		MaxRetries: 1,
	}
	body, status, _, err := b.do(ctx, http.MethodPost, "/task", payload)
	if err != nil {
		return tasks.Task{}, fmt.Errorf("submit task: %w", err)
	}
	if status != http.StatusAccepted {
		return tasks.Task{}, httpStatusError("submit", status, body)
	}

	var task tasks.Task
	if err := json.Unmarshal(body, &task); err != nil {
		return tasks.Task{}, fmt.Errorf("decode submit response: %w", err)
	}
	if task.ID == "" {
		return tasks.Task{}, fmt.Errorf("submit response missing task id")
	}
	return task, nil
}

func (b bridge) getTask(
	ctx context.Context,
	id string,
) (tasks.Task, bool, error) {
	path := "/task/" + url.PathEscape(id)
	body, status, retryable, err := b.do(
		ctx,
		http.MethodGet,
		path,
		nil,
	)
	if err != nil {
		return tasks.Task{}, retryable, err
	}
	if status == http.StatusTooManyRequests || status >= 500 {
		return tasks.Task{}, true, httpStatusError("poll", status, body)
	}
	if status != http.StatusOK {
		return tasks.Task{}, false, httpStatusError("poll", status, body)
	}

	var task tasks.Task
	if err := json.Unmarshal(body, &task); err != nil {
		return tasks.Task{}, false, fmt.Errorf(
			"decode task response: %w",
			err,
		)
	}
	if task.ID != id {
		return tasks.Task{}, false, fmt.Errorf(
			"task response id %q does not match %q",
			task.ID,
			id,
		)
	}
	return task, false, nil
}

func (b bridge) do(
	ctx context.Context,
	method, path string,
	payload any,
) ([]byte, int, bool, error) {
	var reader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, 0, false, fmt.Errorf(
				"encode API request: %w",
				err,
			)
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		b.apiURL+path,
		reader,
	)
	if err != nil {
		return nil, 0, false, fmt.Errorf("create API request: %w", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.apiKey)
	}

	httpResp, err := b.client.Do(req)
	if err != nil {
		return nil, 0, true, fmt.Errorf("call mecha API: %w", err)
	}
	defer httpResp.Body.Close()

	max := b.maxResponseSize
	if max <= 0 {
		max = maxHTTPBodyBytes
	}
	body, err := io.ReadAll(io.LimitReader(httpResp.Body, max+1))
	if err != nil {
		return nil, httpResp.StatusCode, true, fmt.Errorf(
			"read mecha response: %w",
			err,
		)
	}
	if int64(len(body)) > max {
		return nil, httpResp.StatusCode, false, fmt.Errorf(
			"mecha response exceeds %d-byte limit",
			max,
		)
	}
	return body, httpResp.StatusCode, false, nil
}

func httpStatusError(operation string, status int, body []byte) error {
	const maxErrorBody = 4096
	text := strings.TrimSpace(string(body))
	if len(text) > maxErrorBody {
		text = text[:maxErrorBody] + "...[truncated]"
	}
	if text == "" {
		text = http.StatusText(status)
	}
	return fmt.Errorf("%s returned %d: %s", operation, status, text)
}

func extractOutput(raw string) string {
	var result struct {
		Output *string `json:"output"`
	}
	if json.Unmarshal([]byte(raw), &result) == nil && result.Output != nil {
		return *result.Output
	}
	return raw
}
