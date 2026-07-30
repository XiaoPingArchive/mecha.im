package serve

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"mecha.im/internal/logs"
	"mecha.im/internal/events"
	"mecha.im/internal/source"
	"mecha.im/internal/tasks"
	"mecha.im/internal/workers"
	"mecha.im/internal/writeback"
)

// Server is the mecha HTTP daemon that accepts tasks and dispatches to workers.
type Server struct {
	reg          *workers.Registry
	tasks        *tasks.Store
	events       *events.Store
	sources      *source.Registry
	writeback    *writeback.Client
	docker       *workers.DockerClient
	limiter      *RateLimiter
	logs         *logs.Store
	secrets      *workers.Secrets
	pending      chan string
	dispatchWg   sync.WaitGroup
	activeTasks  atomic.Int64
	draining     atomic.Bool
	addr         string
	apiKey       string
	drainTimeout time.Duration
	httpSrv      *http.Server
	logger       *slog.Logger
}

// Config holds server startup parameters.
type Config struct {
	Registry     *workers.Registry
	Tasks        *tasks.Store
	Events       *events.Store
	Sources      *source.Registry
	WriteBack    *writeback.Client
	Docker       *workers.DockerClient
	Limiter      *RateLimiter
	Logs         *logs.Store
	Addr         string
	APIKey       string
	Logger       *slog.Logger
	// DrainTimeout is how long Serve waits for in-flight dispatches after
	// receiving a shutdown signal. Defaults to 10 minutes if zero.
	DrainTimeout time.Duration
	// QueueSize is the pending task channel capacity. Defaults to 256 if zero.
	QueueSize int
}

// New creates a server but does not start it.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Registry == nil || cfg.Tasks == nil {
		panic("serve.New: Registry and Tasks must not be nil")
	}
	drain := cfg.DrainTimeout
	if drain == 0 {
		drain = 10 * time.Minute
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = 256
	}
	s := &Server{
		reg:          cfg.Registry,
		tasks:        cfg.Tasks,
		events:       cfg.Events,
		sources:      cfg.Sources,
		writeback:    cfg.WriteBack,
		docker:       cfg.Docker,
		limiter:      cfg.Limiter,
		logs:         cfg.Logs,
		pending:      make(chan string, queueSize),
		addr:         cfg.Addr,
		apiKey:       cfg.APIKey,
		drainTimeout: drain,
		logger:       cfg.Logger,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /task", s.handlePostTask)
	mux.HandleFunc("GET /task/{id}", s.handleGetTask)
	mux.HandleFunc("GET /tasks", s.handleListTasks)
	mux.HandleFunc("GET /workers", s.handleListWorkers)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", prometheusHandler())
	if s.logs != nil {
		mux.HandleFunc("GET /logs", s.handleLogs)
	}
	if s.sources != nil && s.events != nil {
		mux.HandleFunc("POST /webhook/{source}", s.handleWebhook)
		mux.HandleFunc("GET /webhook/{source}", s.handleWebhook)
		mux.HandleFunc("GET /events", s.handleListEvents)
		mux.HandleFunc("GET /event/{id}", s.handleGetEvent)
	}

	s.httpSrv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.authMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB — prevents oversized header DoS
	}
	return s
}

// Start begins serving HTTP and the dispatch loop. Blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	// Admission/background work follows the caller's lifetime, while tasks
	// already accepted get an independent context so shutdown can drain them.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	taskCtx, cancelTasks := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTasks()

	// Cache secrets once at startup for container builds (M-20: avoid re-reading
	// ~/.mecha/secrets.yml on every disposable container creation).
	if secPath, err := workers.DefaultSecretsPath(); err == nil {
		if sec, err := workers.LoadSecrets(secPath); err == nil {
			s.secrets = sec
		} else {
			s.logger.Warn("load secrets", "err", err)
		}
	}

	// Cleanup orphaned disposable containers from previous crashes
	if s.docker != nil {
		removed, cleanupErr := s.docker.CleanupOrphanDisposables(runCtx)
		if cleanupErr != nil {
			s.logger.Warn("orphan cleanup failed", "err", cleanupErr)
		} else if removed > 0 {
			s.logger.Info("cleaned up orphan disposable containers", "count", removed)
		}
	}

	// Start dispatch loop first so recovered tasks are consumed
	dispatchLoopDone := make(chan struct{})
	go func() {
		defer close(dispatchLoopDone)
		s.dispatchLoopWithTaskContext(runCtx, taskCtx)
	}()

	// Start retry loop — re-enqueues failed tasks after backoff
	go s.retryLoop(runCtx)

	// Start rate limiter cleanup — prevents unbounded bucket growth
	if s.limiter != nil {
		go s.limiterCleanupLoop(runCtx)
	}

	// Start pending scan — catches orphaned tasks not in the channel
	go s.pendingLoop(runCtx)

	// Start write-back retry loop — retries events whose write-back failed
	// transiently (GitHub rate limit, network error). Without this loop,
	// failed write-backs leave events permanently in "dispatched" state.
	if s.events != nil {
		go s.writeBackRetryLoop(runCtx)
	}

	// Start reconciliation loop — detects registry/Docker state drift
	if s.docker != nil {
		go s.reconcileLoop(runCtx, s.docker, 60*time.Second)
	}

	if err := s.recoverTasks(runCtx); err != nil {
		return fmt.Errorf("recover pending tasks: %w", err)
	}
	s.recoverEvents(taskCtx)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.logger.Info("serving", "addr", ln.Addr().String())

	errCh := make(chan error, 1)
	go func() { errCh <- s.httpSrv.Serve(ln) }()

	select {
	case <-ctx.Done():
		s.draining.Store(true)
		stopRun()
		<-dispatchLoopDone // no dispatchWg.Add calls can race with Wait below
		s.logger.Info("shutting down", "drain_timeout", s.drainTimeout)
		hadActiveTasks := s.activeTasks.Load() > 0

		drainTimer := time.NewTimer(s.drainTimeout)
		defer drainTimer.Stop()
		done := make(chan struct{})
		go func() { s.dispatchWg.Wait(); close(done) }()
		drained := false
		select {
		case <-done:
			drained = true
			s.logger.Info("all dispatches drained")
		case <-drainTimer.C:
			s.logger.Warn("shutdown drain timeout — cancelling active dispatches")
			cancelTasks()
			forceTimer := time.NewTimer(30 * time.Second)
			select {
			case <-done:
				s.logger.Info("cancelled dispatches cleaned up")
			case <-forceTimer.C:
				s.logger.Warn("dispatch cleanup timeout")
			}
			if !forceTimer.Stop() {
				select {
				case <-forceTimer.C:
				default:
				}
			}
		}

		// Keep GET /task available briefly so a polling bridge can collect the
		// durable result of a task that completed during the drain.
		if drained && hadActiveTasks {
			resultGrace := time.NewTimer(3 * time.Second)
			<-resultGrace.C
		}

		shutCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelShutdown()
		if err := s.httpSrv.Shutdown(shutCtx); err != nil {
			s.logger.Warn("http shutdown", "err", err)
		}
		return nil
	case err := <-errCh:
		s.draining.Store(true)
		stopRun()
		<-dispatchLoopDone
		cancelTasks()
		done := make(chan struct{})
		go func() { s.dispatchWg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			s.logger.Warn("dispatch cleanup timeout after server error")
		}
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
