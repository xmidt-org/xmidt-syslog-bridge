// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
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

// loadConfig collects the configuration files and env vars and produces a
// validated configuration.
func loadConfig(cli *CLI) (Config, error) {
	gs, err := goschtalt.New(
		goschtalt.StdCfgLayout(applicationName, cli.Files...),
		goschtalt.ConfigIs("two_words"),

		// Seed the program with the default, built-in configuration.
		// Mark this as a default so it is ordered correctly.
		goschtalt.AddValue("built-in", goschtalt.Root, defaultConfig,
			goschtalt.AsDefault()),
	)
	if err != nil {
		return Config{}, err
	}

	if cli.Show {
		// Show the configuration and exit successfully.  Exiting with success
		// matters: if the configuration is broken it is very hard to debug
		// where the problem originates, so being able to see the configuration
		// and then run the service with the same configuration is the point.
		fmt.Fprintln(os.Stdout, gs.Explain().String())

		out, err := gs.Marshal()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		} else {
			fmt.Fprintln(os.Stdout, "## Final Configuration\n---\n"+string(out))
		}

		os.Exit(0)
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
