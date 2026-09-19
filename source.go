// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import "context"

// Source supplies Records and acknowledges them.
//
// The interface exists before its real implementation on purpose.  Everything
// downstream of it -- screening, batching, the commit ordering the delivery
// guarantee rests on -- is testable against a mock, and only the parts that
// genuinely depend on a broker's behavior need one.  See ADR 0001.
type Source interface {
	// Poll blocks until records are available or ctx is done.  It returns no
	// error on an empty result; an empty slice means "nothing yet".
	Poll(ctx context.Context) ([]Record, error)

	// Commit acknowledges every record up to and including those given.  It is
	// called only after the destination write for them succeeded.
	Commit(ctx context.Context, recs []Record) error

	// Pause stops fetching without leaving the consumer group.  Resume undoes
	// it.  Both are idempotent.
	//
	// Leaving the group is what must not happen: during a destination outage
	// every instance is equally unable to write, so a member that dropped out
	// would add a rebalance storm to an outage no restart can fix.  That the
	// group heartbeat survives a pause is verified in heartbeat_test.go.  See
	// ADR 0006.
	Pause()
	Resume()

	Close() error
}
