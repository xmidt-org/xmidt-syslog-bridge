// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

// Package bridge carries syslog messages from an xmidt event source to a
// destination.
//
// This package is the program, minus the program.  It holds the pieces that do
// the work — consuming WRP events, screening the ones that are ours, recovering
// the syslog bytes, batching, and writing to a destination — so that another
// program can embed the same behavior without inheriting this one's
// configuration file format, command line, or HTTP listeners.  Those belong to
// cmd/xmidt-syslog-bridge.
//
// The boundary is drawn so that everything here takes explicit arguments.
// Nothing in this package reads a configuration file, parses a flag, reaches
// for a global, or depends on a dependency injection container.  The command
// translates its configuration into these types; this package never learns
// what a YAML key is called.  That is what keeps the batch lifecycle testable
// without standing up a process.  See docs/design.md and ADR 0007.
package bridge
