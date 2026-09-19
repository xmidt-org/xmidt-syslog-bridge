// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// labelBridge distinguishes two bridges sharing one registry.
const labelBridge = "bridge"

// Two bridges can share one registry as long as the caller distinguishes them,
// which is only possible because newMetrics takes a Registerer it does not own.
func TestNewMetricsTwiceIntoOneRegistryWhenWrapped(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err := newMetrics(prometheus.WrapRegistererWith(
		prometheus.Labels{labelBridge: "a"}, reg))
	require.NoError(t, err)

	_, err = newMetrics(prometheus.WrapRegistererWith(
		prometheus.Labels{labelBridge: "b"}, reg))
	assert.NoError(t, err)
}

// The bridge must not register the Go or process collectors: those describe the
// process and belong to the program, which would otherwise collide with them.
func TestNewMetricsRegistersNoProcessCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	_, err := newMetrics(reg)
	require.NoError(t, err)

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, f := range families {
		assert.NotContains(t, f.GetName(), "go_")
		assert.NotContains(t, f.GetName(), "process_")
	}
}
