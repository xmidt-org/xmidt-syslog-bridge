// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// Bridge carries syslog messages from the event source to the destination.
type Bridge struct {
	cfg        Config
	registerer prometheus.Registerer
	gatherer   prometheus.Gatherer
	logger     *slog.Logger
	metrics    *metrics
	listeners  *listeners
}

// Option configures a Bridge.  Options carry the runtime dependencies, while
// Config carries the values an operator sets; keeping them apart is what lets
// the same Config come from a file, a test, or another program's flags.
type Option interface {
	apply(*Bridge) error
}

type optionFunc func(*Bridge) error

func (f optionFunc) apply(o *Bridge) error { return f(o) }

// WithRegisterer sets where the bridge registers its metrics.  Pass a wrapped
// registerer to distinguish two bridges sharing one registry.
func WithRegisterer(r prometheus.Registerer) Option {
	return optionFunc(func(o *Bridge) error {
		o.registerer = r
		return nil
	})
}

// WithGatherer sets what the metrics listener serves.  It is separate from
// WithRegisterer so that a caller may register through a wrapper while still
// serving the underlying registry.
func WithGatherer(g prometheus.Gatherer) Option {
	return optionFunc(func(o *Bridge) error {
		o.gatherer = g
		return nil
	})
}

// WithLogger sets the logger.  Without one, the bridge is silent.
func WithLogger(l *slog.Logger) Option {
	return optionFunc(func(o *Bridge) error {
		o.logger = l
		return nil
	})
}

// WithRegistry is the common case: register into reg and serve it too.
func WithRegistry(reg *prometheus.Registry) Option {
	return optionFunc(func(o *Bridge) error {
		o.registerer, o.gatherer = reg, reg
		return nil
	})
}

// New builds a Bridge from cfg.  It validates the configuration and registers
// metrics, so an error here means the bridge could never have run.
//
// Nothing is started.  Call Run.
func New(cfg Config, opts ...Option) (*Bridge, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := Bridge{
		cfg:        cfg,
		logger:     slog.New(slog.DiscardHandler),
		registerer: prometheus.NewRegistry(),
	}
	for _, opt := range opts {
		if err := opt.apply(&b); err != nil {
			return nil, err
		}
	}

	var err error
	if b.metrics, err = newMetrics(b.registerer); err != nil {
		return nil, err
	}

	b.listeners = newListeners(cfg.Servers, b.gatherer, b.logger)

	return &b, nil
}

// Run carries messages until ctx is canceled, then shuts down.
//
// The shutdown order is the highest-stakes sequence here and is written out
// rather than derived, because getting it wrong either loses messages or
// commits offsets for messages that were never written:
//
//  1. stop fetching from the event source
//  2. flush every open batch
//  3. commit the offsets those batches covered
//  4. close the destination, then the source
//
// Nothing may be committed before its batch is written.  See ADR 0001.
func (b *Bridge) Run(ctx context.Context) error {
	b.logger.Info("starting",
		"instance", b.cfg.Instance,
		"destination", b.cfg.Destination(),
	)

	// The pipeline is not built yet; the listeners hold the process open.
	err := b.listeners.Run(ctx)

	b.logger.Info("stopped")

	return err
}
