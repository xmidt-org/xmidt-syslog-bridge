// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewListenersSelection(t *testing.T) {
	reg := prometheus.NewRegistry()

	tests := []struct {
		name string
		cfg  Servers
		want int
	}{
		{
			name: "all three configured",
			cfg: Servers{
				Health:  Server{Address: ":1", Path: "/health"},
				Metrics: Server{Address: ":2", Path: "/metrics"},
				Pprof:   Server{Address: ":3", Path: "/debug/pprof"},
			},
			want: 3,
		}, {
			// An empty address is how a listener is turned off.
			name: "pprof disabled by an empty address",
			cfg: Servers{
				Health:  Server{Address: ":1", Path: "/health"},
				Metrics: Server{Address: ":2", Path: "/metrics"},
			},
			want: 2,
		}, {
			name: "nothing configured",
			cfg:  Servers{},
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := newListeners(tc.cfg, reg, nil)

			require.NotNil(t, l)
			assert.Len(t, l.servers, tc.want)
		})
	}
}

// A nil logger must not panic: the logger is a convenience, not a requirement.
func TestNewListenersNilLogger(t *testing.T) {
	l := newListeners(Servers{Health: Server{Address: ":1", Path: "/health"}}, nil, nil)

	require.NotNil(t, l)
	assert.NotNil(t, l.logger)
}

// Without a gatherer there is nothing to serve, so the metrics listener is
// skipped rather than started empty.
func TestNewListenersNoGatherer(t *testing.T) {
	l := newListeners(Servers{Metrics: Server{Address: ":2", Path: "/metrics"}}, nil, nil)

	assert.Empty(t, l.servers)
}

func TestNewListenersGraceDefaults(t *testing.T) {
	assert.Equal(t, defaultShutdownGrace, newListeners(Servers{}, nil, nil).grace)
	assert.Equal(t, defaultShutdownGrace, newListeners(Servers{ShutdownGrace: -1}, nil, nil).grace)
	assert.Equal(t, time.Second, newListeners(Servers{ShutdownGrace: time.Second}, nil, nil).grace)
}

// Liveness is a constant 200 and must never depend on anything downstream.
// See ADR 0006.
func TestHealthHandler(t *testing.T) {
	w := httptest.NewRecorder()
	healthHandler("/health").ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMetricsHandlerServesOurMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_, err := newMetrics(reg)
	require.NoError(t, err)

	w := httptest.NewRecorder()
	metricsHandler("/metrics", reg).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "xmidt_syslog_bridge_records_consumed_total")
}

func TestPprofHandler(t *testing.T) {
	w := httptest.NewRecorder()
	pprofHandler("/debug/pprof").ServeHTTP(w,
		httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

	assert.Equal(t, http.StatusOK, w.Code)
}

// Run must return once the context is canceled, and must not hang on shutdown.
func TestListenersRunStopsOnContextCancel(t *testing.T) {
	l := newListeners(Servers{ShutdownGrace: time.Second}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, l.Run(ctx))
}

// Registering the same metrics twice is a programming error and must be
// reported rather than silently ignored.
func TestNewMetricsRejectsDuplicateRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err := newMetrics(reg)
	require.NoError(t, err)

	_, err = newMetrics(reg)
	assert.Error(t, err)
}
