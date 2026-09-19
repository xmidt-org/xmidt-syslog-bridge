// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The service answers no application traffic.  Its work is consuming from an
// event source and writing to a destination, so the only listeners are health,
// metrics and pprof.

// Server describes one HTTP listener.  An empty Address disables it.
type Server struct {
	Address string
	Path    string
}

// Servers configures the HTTP listeners.  Each is disabled by leaving its
// address empty.
type Servers struct {
	Health  Server
	Metrics Server
	Pprof   Server

	// ShutdownGrace bounds how long in-flight requests have to finish once a
	// shutdown starts.
	ShutdownGrace time.Duration
}

const defaultShutdownGrace = 30 * time.Second

// listeners are the service's HTTP servers, built but not yet running.
type listeners struct {
	servers []*http.Server
	logger  *slog.Logger
	grace   time.Duration
}

// newListeners builds the listeners described by cfg.  The gatherer is what
// the metrics endpoint serves; a nil logger discards.
func newListeners(cfg Servers, g prometheus.Gatherer, logger *slog.Logger) *listeners {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	grace := cfg.ShutdownGrace
	if grace <= 0 {
		grace = defaultShutdownGrace
	}

	l := listeners{logger: logger, grace: grace}

	if cfg.Health.Address != "" {
		l.servers = append(l.servers, cfg.Health.server(healthHandler(cfg.Health.Path)))
	}
	if cfg.Metrics.Address != "" && g != nil {
		l.servers = append(l.servers, cfg.Metrics.server(metricsHandler(cfg.Metrics.Path, g)))
	}
	if cfg.Pprof.Address != "" {
		l.servers = append(l.servers, cfg.Pprof.server(pprofHandler(cfg.Pprof.Path)))
	}

	return &l
}

// Run serves until ctx is canceled or a listener fails, then shuts the rest
// down within the grace period.  A listener that cannot bind is fatal: a
// service whose health or metrics endpoint is silently missing is worse than
// one that refuses to start.
func (l *listeners) Run(ctx context.Context) error {
	fatal := make(chan error, len(l.servers))

	for _, srv := range l.servers {
		go func() {
			l.logger.Info("listening", "address", srv.Addr)
			err := srv.ListenAndServe()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				fatal <- err
			}
		}()
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-fatal:
	}

	shutdown, cancel := context.WithTimeout(context.Background(), l.grace)
	defer cancel()

	for _, srv := range l.servers {
		if e := srv.Shutdown(shutdown); e != nil {
			l.logger.Error("shutdown failed", "address", srv.Addr, "error", e)
		}
	}

	return err
}

// healthHandler serves liveness.
//
// It is deliberately a constant 200.  Liveness reports that the process is up
// and must NOT depend on whether the destination is reachable.  During a
// destination outage every instance in the consumer group is equally unable to
// write, so a probe that failed would restart-loop the whole group and add a
// rebalance storm to an outage that no restart can fix.  Destination trouble
// surfaces through metrics instead.  See ADR 0006.
func healthHandler(path string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func metricsHandler(path string, g prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET "+path, promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	return mux
}

func pprofHandler(path string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(path+"/", pprof.Index)
	mux.HandleFunc(path+"/cmdline", pprof.Cmdline)
	mux.HandleFunc(path+"/profile", pprof.Profile)
	mux.HandleFunc(path+"/symbol", pprof.Symbol)
	mux.HandleFunc(path+"/trace", pprof.Trace)
	return mux
}

func (s Server) server(h http.Handler) *http.Server {
	return &http.Server{
		Addr:              s.Address,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}
}
