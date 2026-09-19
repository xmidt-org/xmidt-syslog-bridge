// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import "context"

// Destination writes Syslog Messages.  Write returns only once the messages are
// durable; returning nil is what permits their offsets to be committed.
//
// Batching is NOT a destination concern.  The pipeline decides when to call
// Write and with how many messages: one at a time for syslog, a whole Batch for
// S3.  That is what lets the batch engine be tested against a mock destination
// and no S3 at all.
type Destination interface {
	// Write delivers msgs.  A failure that no retry could fix must be returned
	// wrapped in Permanent(); everything else is retried forever.  See ADR 0004.
	Write(ctx context.Context, msgs []Message) error

	// Name is "syslog" or "s3", for metric labels and log lines.
	Name() string

	Close() error
}
