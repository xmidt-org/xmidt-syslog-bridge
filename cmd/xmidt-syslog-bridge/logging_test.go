// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bridge "github.com/xmidt-org/xmidt-syslog-bridge"
)

const stdout = "stdout"

func TestLoggingBuild(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Logging
		dev         bool
		expectErr   bool
		errContains string
	}{
		{
			name: "json to stdout",
			cfg:  Logging{Level: "info", Encoding: "json", Output: stdout},
		}, {
			name: "text to stderr",
			cfg:  Logging{Level: "debug", Encoding: "text", Output: "stderr"},
		}, {
			// Empty encoding and output are the documented defaults rather
			// than errors, so a minimal config still produces a logger.
			name: "empty encoding and output default",
			cfg:  Logging{Level: "warn"},
		}, {
			name: "case does not matter",
			cfg:  Logging{Level: "error", Encoding: "JSON", Output: "STDOUT"},
		}, {
			// Development mode ignores the rest, so a configuration that would
			// otherwise be rejected still yields a usable logger.
			name: "dev mode overrides everything",
			cfg:  Logging{Level: "nonsense", Encoding: "nonsense", Output: "nonsense"},
			dev:  true,
		}, {
			name:        "unknown level",
			cfg:         Logging{Level: "chatty", Encoding: "json", Output: stdout},
			expectErr:   true,
			errContains: "logging.level",
		}, {
			name:        "unknown output",
			cfg:         Logging{Level: "info", Encoding: "json", Output: "/var/log/app.log"},
			expectErr:   true,
			errContains: "logging.output",
		}, {
			name:        "unknown encoding",
			cfg:         Logging{Level: "info", Encoding: "xml", Output: stdout},
			expectErr:   true,
			errContains: "logging.encoding",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l, err := tc.cfg.build(tc.dev)

			if !tc.expectErr {
				require.NoError(t, err)
				assert.NotNil(t, l)
				return
			}

			assert.Error(t, err)
			assert.ErrorIs(t, err, bridge.ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.errContains)
			assert.Nil(t, l)
		})
	}
}

// Every level name zap and slog accept must round trip, since operators copy
// these from other services.
func TestLoggingLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "DEBUG", "Info"} {
		t.Run(level, func(t *testing.T) {
			_, err := Logging{Level: level, Encoding: "json", Output: stdout}.build(false)
			assert.NoError(t, err)
		})
	}
}
