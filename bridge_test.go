// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"context"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewValidatesConfig(t *testing.T) {
	// No destination: the bridge could never have run, so New must refuse.
	_, err := New(base())

	assert.ErrorIs(t, err, ErrInvalidConfig)
}

func TestNewWithNoOptions(t *testing.T) {
	// Every dependency is optional: a bridge with no options is silent and
	// registers into a registry of its own.
	b, err := New(with(func(c *Config) { c.Syslog = validSyslog() }))

	require.NoError(t, err)
	assert.NotNil(t, b.logger)
	assert.NotNil(t, b.metrics)
}

func TestNewRegistersIntoTheGivenRegisterer(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err := New(with(func(c *Config) { c.Syslog = validSyslog() }),
		WithRegistry(reg))
	require.NoError(t, err)

	families, err := reg.Gather()
	require.NoError(t, err)
	assert.NotEmpty(t, families)
}

// Registering through a wrapper while serving the underlying registry is why
// WithRegisterer and WithGatherer are separate options.
func TestWithRegistererAndGathererAreIndependent(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err := New(with(func(c *Config) { c.Syslog = validSyslog() }),
		WithRegisterer(prometheus.WrapRegistererWith(
			prometheus.Labels{labelBridge: "a"}, reg)),
		WithGatherer(reg),
	)
	require.NoError(t, err)

	_, err = New(with(func(c *Config) { c.Syslog = validSyslog() }),
		WithRegisterer(prometheus.WrapRegistererWith(
			prometheus.Labels{labelBridge: "b"}, reg)),
		WithGatherer(reg),
	)
	assert.NoError(t, err, "two bridges must coexist in one registry when wrapped")
}

func TestNewSurfacesDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := with(func(c *Config) { c.Syslog = validSyslog() })

	_, err := New(cfg, WithRegistry(reg))
	require.NoError(t, err)

	_, err = New(cfg, WithRegistry(reg))
	assert.Error(t, err, "an unwrapped second bridge must not silently share metrics")
}

func TestRunStopsOnContextCancel(t *testing.T) {
	b, err := New(with(func(c *Config) { c.Syslog = validSyslog() }),
		WithLogger(slog.New(slog.DiscardHandler)))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, b.Run(ctx))
}

// New must return the Bridge it applied the options to.  An earlier version
// built one Bridge, applied the options to it, and returned a different
// literal, so the logger was nil and Run panicked on its first log line.
func TestNewKeepsTheOptionsItApplied(t *testing.T) {
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.DiscardHandler)

	b, err := New(with(func(c *Config) { c.Syslog = validSyslog() }),
		WithRegistry(reg),
		WithLogger(logger),
	)

	require.NoError(t, err)
	assert.Same(t, logger, b.logger)
	assert.Same(t, reg, b.registerer)
	assert.Same(t, reg, b.gatherer)
	assert.NotNil(t, b.metrics)
	assert.NotNil(t, b.listeners)
}
