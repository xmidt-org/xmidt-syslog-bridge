// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bridge "github.com/xmidt-org/xmidt-syslog-bridge"
)

const minimalConfig = `---
instance: test-instance
kafka:
    brokers: [localhost:9092]
    topic: apparmor-alerts
    group: xmidt-syslog-bridge
filter:
    event: apparmor
syslog:
    network: unixgram
    address: /dev/log
servers:
    health:
        address: ""
    metrics:
        address: ""
    pprof:
        address: ""
`

// write puts content in a temp file and returns its path.
func write(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))

	return path
}

func TestParseCLI(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expectErr bool
		check     func(*testing.T, *CLI)
	}{
		{
			name:  "no arguments",
			args:  nil,
			check: func(t *testing.T, c *CLI) { assert.False(t, c.Dev); assert.False(t, c.Show) },
		}, {
			name:  "dev and show",
			args:  []string{"-d", "-s"},
			check: func(t *testing.T, c *CLI) { assert.True(t, c.Dev); assert.True(t, c.Show) },
		}, {
			name:  "repeated files accumulate",
			args:  []string{"-f", "a.yml", "-f", "b.yml"},
			check: func(t *testing.T, c *CLI) { assert.Equal(t, []string{"a.yml", "b.yml"}, c.Files) },
		}, {
			// An unknown flag must be reported rather than ignored, or a
			// typo silently changes how the service runs.
			name:      "unknown flag",
			args:      []string{"--nope"},
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli, err := parseCLI(tc.args)

			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			tc.check(t, cli)
		})
	}
}

func TestNewRegistryCarriesProcessCollectors(t *testing.T) {
	reg := newRegistry()
	require.NotNil(t, reg)

	families, err := reg.Gather()
	require.NoError(t, err)

	var go_, process bool
	for _, f := range families {
		switch {
		case len(f.GetName()) > 3 && f.GetName()[:3] == "go_":
			go_ = true
		case len(f.GetName()) > 8 && f.GetName()[:8] == "process_":
			process = true
		}
	}

	assert.True(t, go_, "the program owns the Go collector")
	assert.True(t, process, "the program owns the process collector")
}

// run with start=false assembles everything without serving, which is the
// whole startup path short of binding a port.
func TestRunAssembles(t *testing.T) {
	path := write(t, "xmidt-syslog-bridge.yml", minimalConfig)

	assert.NoError(t, run([]string{"-f", path}, false))
}

func TestRunErrors(t *testing.T) {
	tests := []struct {
		name        string
		args        func(*testing.T) []string
		errIs       error
		errContains string
	}{
		{
			name:  "unknown flag",
			args:  func(*testing.T) []string { return []string{"--nope"} },
			errIs: nil,
		}, {
			// A file named explicitly is meant to be used, so a missing one is
			// fatal rather than skipped in favor of built-in defaults.
			name:        "named file does not exist",
			args:        func(*testing.T) []string { return []string{"-f", "/no/such/file.yml"} },
			errIs:       bridge.ErrInvalidConfig,
			errContains: "cannot read",
		}, {
			name: "named file is unreadable",
			args: func(t *testing.T) []string {
				p := write(t, "secret.yml", minimalConfig)
				require.NoError(t, os.Chmod(p, 0000))
				return []string{"-f", p}
			},
			errIs:       bridge.ErrInvalidConfig,
			errContains: "cannot read",
		}, {
			name: "malformed yaml",
			args: func(t *testing.T) []string {
				return []string{"-f", write(t, "bad.yml", "---\nkafka:\n  brokers: [unterminated\n")}
			},
			errIs: bridge.ErrInvalidConfig,
		}, {
			name: "no destination configured",
			args: func(t *testing.T) []string {
				return []string{"-f", write(t, "nodest.yml", `---
kafka:
    brokers: [localhost:9092]
    topic: t
    group: g
filter:
    event: apparmor
`)}
			},
			errIs:       bridge.ErrInvalidConfig,
			errContains: "no destination configured",
		}, {
			name: "bad logging level",
			args: func(t *testing.T) []string {
				return []string{"-f", write(t, "badlog.yml", minimalConfig+`
logging:
    level: chatty
`)}
			},
			errIs:       bridge.ErrInvalidConfig,
			errContains: "logging.level",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := run(tc.args(t), false)

			require.Error(t, err)
			if tc.errIs != nil {
				assert.ErrorIs(t, err, tc.errIs)
			}
			if tc.errContains != "" {
				assert.Contains(t, err.Error(), tc.errContains)
			}
		})
	}
}

// -s/--show must report through a sentinel rather than exiting inside
// loadConfig, so that exiting stays main's decision.
func TestRunShowConfig(t *testing.T) {
	path := write(t, "xmidt-syslog-bridge.yml", minimalConfig)

	err := run([]string{"-f", path, "-s"}, false)

	assert.ErrorIs(t, err, errConfigShown)
}

// Showing a broken configuration must still succeed: it is the one tool for
// debugging a broken configuration.
func TestRunShowWorksOnBrokenConfig(t *testing.T) {
	path := write(t, "broken.yml", "---\nkafka:\n    topic: no-brokers-here\n")

	err := run([]string{"-f", path, "-s"}, false)

	assert.ErrorIs(t, err, errConfigShown)
}

func TestReport(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", err: nil, want: 0},
		// Showing the configuration is a successful outcome, not a failure.
		{name: "configuration shown", err: errConfigShown, want: 0},
		{name: "wrapped configuration shown", err: fmt.Errorf("x: %w", errConfigShown), want: 0},
		{name: "bad configuration", err: bridge.ErrInvalidConfig, want: -1},
		{name: "anything else", err: errors.New("boom"), want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, report(tc.err))
		})
	}
}

// The serving path: run with start=true must come back when the process is
// signaled, which is what lets a container stop cleanly rather than being
// killed mid-batch.
func TestRunServesUntilSignalled(t *testing.T) {
	// Disarm the default action first, so a signal that arrives before run
	// installs its own handler cannot kill the test binary.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	path := write(t, "xmidt-syslog-bridge.yml", minimalConfig)

	done := make(chan error, 1)
	go func() { done <- run([]string{"-f", path}, true) }()

	// run installs its handler asynchronously, so keep signaling until it
	// returns rather than racing a single delivery.
	for {
		select {
		case err := <-done:
			assert.NoError(t, err)
			return
		case <-time.After(20 * time.Millisecond):
			require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGTERM))
		}
	}
}
