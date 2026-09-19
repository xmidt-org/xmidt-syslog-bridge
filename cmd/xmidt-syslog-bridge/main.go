// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	bridge "github.com/xmidt-org/xmidt-syslog-bridge"
)

const applicationName = "xmidt-syslog-bridge"

// These match what goreleaser provides.
var (
	commit  = "undefined"
	version = "undefined"
	date    = "undefined"
	builtBy = "undefined"
)

// CLI is the structure that is used to capture the command line arguments.
type CLI struct {
	Dev   bool     `optional:"" short:"d" help:"Run in development mode."`
	Show  bool     `optional:"" short:"s" help:"Show the configuration and exit."`
	Files []string `optional:"" short:"f" help:"Specific configuration files or directories."`
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("stacktrace from panic: \n" + string(debug.Stack()))
		}
	}()

	if err := run(os.Args[1:], true); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, bridge.ErrInvalidConfig) {
			fmt.Fprintln(os.Stderr, "Run with -s/--show to see the configuration.")
		}
		os.Exit(-1)
	}
}

// run assembles the service and, when start is true, runs it until signaled.
//
// The wiring is deliberately explicit.  The graph is small and almost linear,
// and the shutdown order at the bottom is the highest-stakes sequence in the
// service, so it is written out to be read rather than derived from a
// container.  See ADR 0007.
func run(args []string, start bool) error {
	cli, err := parseCLI(args)
	if err != nil {
		return err
	}

	cfg, err := loadConfig(cli)
	if err != nil {
		return err
	}

	logger, err := cfg.Logging.build(cli.Dev)
	if err != nil {
		return err
	}
	logger.Info("build",
		"version", version,
		"commit", commit,
		"date", date,
		"built_by", builtBy,
	)

	b, err := bridge.New(cfg.bridgeConfig(),
		bridge.WithRegistry(newRegistry()),
		bridge.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	if !start {
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Shutdown order, once the pipeline exists, is:
	//   1. stop fetching from Kafka
	//   2. flush every open batch
	//   3. commit the offsets those batches covered
	//   4. close the destination, then the Kafka client
	// Nothing may be committed before its batch is written.  See ADR 0001.
	return b.Run(ctx)
}

func parseCLI(args []string) (*CLI, error) {
	var cli CLI

	parser, err := kong.New(&cli,
		kong.Name(applicationName),
		kong.Description("Delivers syslog messages carried over WRP to a syslog or S3 destination.\n"+
			fmt.Sprintf("\tVersion:  %s\n", version)+
			fmt.Sprintf("\tDate:     %s\n", date)+
			fmt.Sprintf("\tCommit:   %s\n", commit)+
			fmt.Sprintf("\tBuilt By: %s\n", builtBy),
		),
		kong.UsageOnError(),
	)
	if err != nil {
		return nil, err
	}

	if _, err = parser.Parse(args); err != nil {
		parser.FatalIfErrorf(err)
	}

	return &cli, nil
}

// newRegistry returns a registry carrying the standard Go and process
// collectors alongside whatever the program registers.
//
// This lives here rather than in package bridge because both collectors
// describe the process, not the bridge.  Only one thing in a process may
// register them -- a second attempt is an AlreadyRegisteredError -- so the
// program owns them, and the bridge registers only its own instruments into
// whatever Registerer it is handed.
func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}
