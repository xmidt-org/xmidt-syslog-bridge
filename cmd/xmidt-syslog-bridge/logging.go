// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	bridge "github.com/xmidt-org/xmidt-syslog-bridge"
)

// Logging configures this service's own logs.  They go to stdout by default
// and must never be pointed at the syslog destination: with a local /dev/log
// that is a feedback loop, where a delivery error logs a message that is
// itself delivered.
type Logging struct {
	// Level is one of debug, info, warn, error.
	Level string

	// Encoding is json or text.
	Encoding string

	// Output is stdout or stderr.
	Output string
}

// build turns the configuration into a logger.  In development mode the
// settings are overridden for something readable at a terminal.
func (l Logging) build(dev bool) (*slog.Logger, error) {
	if dev {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		})), nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(l.Level)); err != nil {
		return nil, fmt.Errorf("%w: logging.level %q is not one of debug, info, warn, error",
			bridge.ErrInvalidConfig, l.Level)
	}

	var w io.Writer
	switch strings.ToLower(l.Output) {
	case "stdout", "":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	default:
		return nil, fmt.Errorf("%w: logging.output %q is not one of stdout, stderr",
			bridge.ErrInvalidConfig, l.Output)
	}

	opts := slog.HandlerOptions{Level: level}

	switch strings.ToLower(l.Encoding) {
	case "json", "":
		return slog.New(slog.NewJSONHandler(w, &opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, &opts)), nil
	}

	return nil, fmt.Errorf("%w: logging.encoding %q is not one of json, text",
		bridge.ErrInvalidConfig, l.Encoding)
}
