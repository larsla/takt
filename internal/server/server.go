// Package server wires the engine to NATS: the frontend API as a NATS micro
// service, consumers for fired timers and transfers, and the sweeper.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"

	"github.com/larsla/takt/api"
	"github.com/larsla/takt/internal/server/engine"
)

// Version is reported by the micro service.
const Version = "0.1.0"

// Config configures a Server.
type Config struct {
	NC     *nats.Conn
	JS     jetstream.JetStream
	Logger *slog.Logger
	// SweepInterval is how often interrupted commits are retried. Defaults to 30s.
	SweepInterval time.Duration
	// Engine tunes the engine; NC, JS and Logger are filled in from above.
	Engine engine.Config
	// MaxConcurrent bounds in-flight API calls, timers and transfers. Defaults to 256.
	MaxConcurrent int
}

// Server is a running takt server.
type Server struct {
	e      *engine.Engine
	log    *slog.Logger
	svc    micro.Service
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	sem    chan struct{}
	cons   []jetstream.ConsumeContext
}

const requestTimeout = 2 * time.Minute

// Start starts serving. The streams and buckets must already exist.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = 30 * time.Second
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 256
	}
	ecfg := cfg.Engine
	ecfg.NC, ecfg.JS, ecfg.Logger = cfg.NC, cfg.JS, cfg.Logger
	e, err := engine.New(ctx, ecfg)
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithCancel(context.Background())
	s := &Server{e: e, log: cfg.Logger, ctx: sctx, cancel: cancel, sem: make(chan struct{}, cfg.MaxConcurrent)}

	if err := s.startConsumer(ctx, cfg.JS, api.TimerStream, "takt_timers", engine.TimerFireSubjects, e.HandleTimer); err != nil {
		s.Stop()
		return nil, err
	}
	if err := s.startConsumer(ctx, cfg.JS, api.TransferStream, "takt_transfer", engine.TransferSubjects, e.HandleTransfer); err != nil {
		s.Stop()
		return nil, err
	}
	if err := s.startService(cfg.NC); err != nil {
		s.Stop()
		return nil, err
	}
	s.wg.Go(func() { s.sweepLoop(cfg.SweepInterval) })
	return s, nil
}

// Engine returns the engine, for in-process callers such as the web UI.
func (s *Server) Engine() *engine.Engine { return s.e }

// Stop stops serving and waits for in-flight work.
func (s *Server) Stop() {
	if s.svc != nil {
		_ = s.svc.Stop()
	}
	for _, cc := range s.cons {
		cc.Stop()
	}
	s.cancel()
	s.wg.Wait()
}

// run executes f in a goroutine, bounded by the concurrency limit.
func (s *Server) run(f func()) {
	s.sem <- struct{}{}
	s.wg.Go(func() {
		defer func() { <-s.sem }()
		f()
	})
}

func (s *Server) startConsumer(ctx context.Context, js jetstream.JetStream, stream, name, filter string, handle func(context.Context, []byte) error) error {
	cons, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       name,
		FilterSubject: filter,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		MaxDeliver:    -1,
		MaxAckPending: 4096,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", name, err)
	}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		s.run(func() {
			if err := handle(s.ctx, msg.Data()); err != nil {
				s.log.Warn("processing failed, will retry", "consumer", name, "subject", msg.Subject(), "err", err)
				_ = msg.NakWithDelay(time.Second)
				return
			}
			_ = msg.Ack()
		})
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", name, err)
	}
	s.cons = append(s.cons, cc)
	return nil
}

func (s *Server) sweepLoop(every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
			if n, err := s.e.Sweep(s.ctx); err != nil {
				s.log.Warn("sweep", "err", err)
			} else if n > 0 {
				s.log.Info("sweep finalized interrupted commits", "runs", n)
			}
		}
	}
}

func (s *Server) startService(nc *nats.Conn) error {
	svc, err := micro.AddService(nc, micro.Config{
		Name:        "takt",
		Version:     Version,
		Description: "takt workflow service",
		QueueGroup:  "takt",
	})
	if err != nil {
		return err
	}
	s.svc = svc
	g := svc.AddGroup(strings.TrimSuffix(api.APISubjectPrefix, "."))
	e := s.e
	endpoints := map[string]micro.Handler{
		api.MethodRegisterDomain:                   handleVoid(s, e.RegisterDomain),
		api.MethodDescribeDomain:                   handle(s, e.DescribeDomain),
		api.MethodUpdateDomain:                     handle(s, e.UpdateDomain),
		api.MethodListDomains:                      handle(s, e.ListDomains),
		api.MethodStartWorkflowExecution:           handle(s, e.StartWorkflowExecution),
		api.MethodSignalWorkflowExecution:          handleVoid(s, e.SignalWorkflowExecution),
		api.MethodSignalWithStartWorkflowExecution: handle(s, e.SignalWithStartWorkflowExecution),
		api.MethodRequestCancelWorkflowExecution:   handleVoid(s, e.RequestCancelWorkflowExecution),
		api.MethodTerminateWorkflowExecution:       handleVoid(s, e.TerminateWorkflowExecution),
		api.MethodGetWorkflowExecutionHistory:      handle(s, e.GetWorkflowExecutionHistory),
		api.MethodDescribeWorkflowExecution:        handle(s, e.DescribeWorkflowExecution),
		api.MethodQueryWorkflow:                    handle(s, e.QueryWorkflow),
		api.MethodRecordDecisionTaskStarted:        handle(s, e.RecordDecisionTaskStarted),
		api.MethodRespondDecisionTaskCompleted:     handle(s, e.RespondDecisionTaskCompleted),
		api.MethodRespondDecisionTaskFailed:        handleVoid(s, e.RespondDecisionTaskFailed),
		api.MethodRecordActivityTaskStarted:        handle(s, e.RecordActivityTaskStarted),
		api.MethodRecordActivityTaskHeartbeat:      handle(s, e.RecordActivityTaskHeartbeat),
		api.MethodRecordActivityTaskHeartbeatByID:  handle(s, e.RecordActivityTaskHeartbeatByID),
		api.MethodRespondActivityTaskCompleted:     handleVoid(s, e.RespondActivityTaskCompleted),
		api.MethodRespondActivityTaskCompletedByID: handleVoid(s, e.RespondActivityTaskCompletedByID),
		api.MethodRespondActivityTaskFailed:        handleVoid(s, e.RespondActivityTaskFailed),
		api.MethodRespondActivityTaskFailedByID:    handleVoid(s, e.RespondActivityTaskFailedByID),
		api.MethodRespondActivityTaskCanceled:      handleVoid(s, e.RespondActivityTaskCanceled),
		api.MethodRespondActivityTaskCanceledByID:  handleVoid(s, e.RespondActivityTaskCanceledByID),
		api.MethodListOpenWorkflowExecutions:       handle(s, e.ListOpenWorkflowExecutions),
		api.MethodListClosedWorkflowExecutions:     handle(s, e.ListClosedWorkflowExecutions),
		api.MethodDescribeTaskList:                 handle(s, e.DescribeTaskList),
		api.MethodListTaskLists:                    handle(s, e.ListTaskLists),
		api.MethodRecordPoller:                     handleVoid(s, e.RecordPoller),
	}
	for name, h := range endpoints {
		if err := g.AddEndpoint(name, h); err != nil {
			return fmt.Errorf("endpoint %s: %w", name, err)
		}
	}
	return nil
}

// handle adapts an engine method to a micro handler. Each request runs in
// its own goroutine so long polls don't block the endpoint.
func handle[Req, Resp any](s *Server, f func(context.Context, *Req) (Resp, error)) micro.Handler {
	return micro.HandlerFunc(func(r micro.Request) {
		s.run(func() {
			var req Req
			if err := json.Unmarshal(r.Data(), &req); err != nil {
				s.respondError(r, &api.BadRequestError{Message: "malformed request: " + err.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(s.ctx, requestTimeout)
			defer cancel()
			resp, err := f(ctx, &req)
			if err != nil {
				s.respondError(r, err)
				return
			}
			if err := r.RespondJSON(resp); err != nil {
				s.log.Warn("respond", "subject", r.Subject(), "err", err)
			}
		})
	})
}

func handleVoid[Req any](s *Server, f func(context.Context, *Req) error) micro.Handler {
	return handle(s, func(ctx context.Context, req *Req) (struct{}, error) {
		return struct{}{}, f(ctx, req)
	})
}

func (s *Server) respondError(r micro.Request, err error) {
	code, data := api.EncodeError(err)
	var ise *api.InternalServiceError
	if code == "InternalServiceError" && !errors.As(err, &ise) {
		s.log.Error("request failed", "subject", r.Subject(), "err", err)
	}
	desc := strings.ReplaceAll(err.Error(), "\n", " ")
	if rerr := r.Error(code, desc, data); rerr != nil {
		s.log.Warn("respond error", "subject", r.Subject(), "err", rerr)
	}
}
