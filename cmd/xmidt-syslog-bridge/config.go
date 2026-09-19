// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/goschtalt/goschtalt"

	// Register the decoders and encoders goschtalt needs.  Without the yaml
	// decoder every configuration file is silently ignored and the service
	// runs on built-in defaults alone.
	_ "github.com/goschtalt/goschtalt/pkg/typical"
	_ "github.com/goschtalt/yaml-decoder"
	_ "github.com/goschtalt/yaml-encoder"

	bridge "github.com/xmidt-org/xmidt-syslog-bridge"
)

// Config is this program's configuration file format.  It is the bridge's
// configuration plus the things only a standalone program needs, kept flat so
// the YAML has no redundant nesting.
type Config struct {
	Logging Logging

	Instance string
	Servers  bridge.Servers
	Kafka    bridge.Kafka
	Filter   bridge.Filter
	Syslog   *bridge.Syslog
	S3       *bridge.S3
}

// bridgeConfig translates the file format into what the bridge takes.  The
// translation is explicit and dull on purpose: it is the seam that keeps the
// bridge from ever learning what a YAML key is called.
func (c Config) bridgeConfig() bridge.Config {
	return bridge.Config{
		Instance: c.Instance,
		Servers:  c.Servers,
		Kafka:    c.Kafka,
		Filter:   c.Filter,
		Syslog:   c.Syslog,
		S3:       c.S3,
	}
}

// errConfigShown reports that -s/--show printed the configuration and the
// program should stop, successfully.  It is a sentinel rather than an
// os.Exit deep in the call stack so that the path stays testable and so that
// exiting remains main's decision.
var errConfigShown = errors.New("configuration shown")

// loadConfig collects the configuration files and env vars and produces a
// validated configuration.
func loadConfig(cli *CLI) (Config, error) {
	// goschtalt skips a file it cannot read, which turns a permissions or typo
	// problem into a confusing complaint about a missing setting much later.
	// A file named explicitly is meant to be used, so failing to read one is
	// fatal here.
	for _, f := range cli.Files {
		h, err := os.Open(f) // nolint: gosec // the operator named this path
		if err != nil {
			return Config{}, fmt.Errorf("%w: cannot read %s: %s",
				bridge.ErrInvalidConfig, f, err)
		}
		_ = h.Close()
	}

	gs, err := goschtalt.New(
		goschtalt.StdCfgLayout(applicationName, cli.Files...),
		goschtalt.ConfigIs("two_words"),

		// Seed the program with the default, built-in configuration.
		// Mark this as a default so it is ordered correctly.
		goschtalt.AddValue("built-in", goschtalt.Root, defaultConfig,
			goschtalt.AsDefault()),
	)
	if err != nil {
		// Wrapped so that a decoder error -- a YAML syntax mistake, most
		// often -- reaches the operator with the same guidance as any other
		// configuration problem.
		return Config{}, fmt.Errorf("%w: %s", bridge.ErrInvalidConfig, err)
	}

	if cli.Show {
		// Showing the configuration succeeds even when the configuration is
		// broken: if it were an error, the one tool for debugging a broken
		// configuration would refuse to run exactly when it is needed.
		fmt.Fprintln(os.Stdout, gs.Explain().String())

		out, err := gs.Marshal()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Fprintln(os.Stdout, "## Final Configuration\n---\n"+string(out))
		}

		return Config{}, errConfigShown
	}

	var cfg Config
	if err := gs.Unmarshal(goschtalt.Root, &cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %s", bridge.ErrInvalidConfig, err)
	}

	if cfg.Instance == "" {
		if host, err := os.Hostname(); err == nil {
			cfg.Instance = host
		}
	}

	// Validated here rather than at New so a bad configuration is reported
	// before anything else is built.
	if err := cfg.bridgeConfig().Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// -----------------------------------------------------------------------------
// Keep the default configuration at the bottom of the file so it is easy to
// see what the default configuration is.
// -----------------------------------------------------------------------------

var defaultConfig = Config{
	Logging: Logging{
		Level:    "info",
		Encoding: "json",
		Output:   "stdout",
	},
	Servers: bridge.Servers{
		Health:        bridge.Server{Address: ":10080", Path: "/health"},
		Metrics:       bridge.Server{Address: ":9361", Path: "/metrics"},
		Pprof:         bridge.Server{Address: "127.0.0.1:9999", Path: "/debug/pprof"},
		ShutdownGrace: 30 * time.Second,
	},
}
