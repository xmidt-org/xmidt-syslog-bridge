// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The test doubles every slice from WI-03 on builds on, so a change here is a
// change to every slice.
//
// Nothing in this file needs a container, which is why it carries no build tag.
// The Kafka fixture that does is in fixture_integration_test.go.  A pure helper
// belongs on this side even when its only caller is tagged: dockerSkipReason is
// the example, so that it keeps a test in the untagged suite.

// dockerSkipReason turns a probe result into the message requireDocker skips
// with, or "" when the runtime is reachable.
//
// The reason is not decoration.  A suite that quietly skipped its integration
// tests looks exactly like one that ran them, and the whole point of not using
// a build tag is that the skip is the only thing that says which happened.
func dockerSkipReason(err error) string {
	if err == nil {
		return ""
	}

	return "docker unavailable: " + err.Error()
}

// -----------------------------------------------------------------------------
// The syslog sink
// -----------------------------------------------------------------------------

// newSyslogSink listens on a Unix datagram socket and records the raw bytes of
// every datagram it receives, in order.  It is what byte-identical delivery is
// asserted against.
//
// The socket lives under t.TempDir(): /dev/log is not writable in a test, and a
// fixed path would collide between tests running in parallel.
func newSyslogSink(t *testing.T) (string, func() [][]byte) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "s")

	// A Unix socket path is bounded by the kernel at 108 bytes, and a long
	// subtest name pushes t.TempDir() past it.  The failure is an opaque
	// "invalid argument" from bind, so fall back before provoking it.
	if len(path) > 100 {
		dir, err := os.MkdirTemp("", "sink")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		path = filepath.Join(dir, "s")
	}

	conn, err := net.ListenPacket("unixgram", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	var (
		mu       sync.Mutex
		received [][]byte
	)

	go func() {
		// Larger than anything a test sends, so a datagram is never truncated
		// into looking like a delivery bug.
		buf := make([]byte, 1<<20)
		for {
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}

			mu.Lock()
			received = append(received, bytes.Clone(buf[:n]))
			mu.Unlock()
		}
	}()

	return path, func() [][]byte {
		mu.Lock()
		defer mu.Unlock()

		return slices.Clone(received)
	}
}

// -----------------------------------------------------------------------------
// mockSource
// -----------------------------------------------------------------------------

// mockSource and mockDest are the Source and Destination every later slice
// tests against, so the compiler is made to check that they still are.
var (
	_ Source      = (*mockSource)(nil)
	_ Destination = (*mockDest)(nil)
)

// mockSource serves canned records and records what was committed.
//
// Its behavior follows the Source contract rather than what is convenient.
// When the canned records run out, Poll blocks until its context is done, which
// is what a real consumer on an idle topic does; a mock that reported
// exhaustion would let a pipeline pass a shutdown test that it would fail
// against Kafka.  Set drain when a test wants the loop to end on its own.
type mockSource struct {
	mu        sync.Mutex
	records   [][]Record
	committed []Record
	paused    bool
	closed    bool

	// drain makes Poll return an empty batch once the canned records run out,
	// rather than blocking.  An empty batch is "nothing yet", not an error.
	drain bool

	// resume wakes a Poll that is waiting out a pause.
	resume chan struct{}
}

func newMockSource(batches ...[]Record) *mockSource {
	return &mockSource{records: batches, resume: make(chan struct{}, 1)}
}

func (m *mockSource) Poll(ctx context.Context) ([]Record, error) {
	for {
		if recs, ok := m.next(); ok {
			return recs, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.resume:
		}
	}
}

// next returns the next canned batch, or false when there is nothing to serve
// right now -- because the source is paused, or because its records ran out.
func (m *mockSource) next() ([]Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.paused {
		return nil, false
	}

	if len(m.records) > 0 {
		recs := m.records[0]
		m.records = m.records[1:]

		return recs, true
	}

	return nil, m.drain
}

func (m *mockSource) Commit(_ context.Context, recs []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.committed = append(m.committed, recs...)

	return nil
}

func (m *mockSource) Pause() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.paused = true
}

func (m *mockSource) Resume() {
	m.mu.Lock()
	m.paused = false
	m.mu.Unlock()

	// Non-blocking: Resume is idempotent and must not depend on anyone polling.
	select {
	case m.resume <- struct{}{}:
	default:
	}
}

func (m *mockSource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	return nil
}

func (m *mockSource) commits() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.committed)
}

func (m *mockSource) isPaused() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.paused
}

func (m *mockSource) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closed
}

// -----------------------------------------------------------------------------
// mockDest
// -----------------------------------------------------------------------------

// mockDest records what it was asked to write and can be told to fail.  Wrap
// its error in Permanent() to drive the drop path.
type mockDest struct {
	mu     sync.Mutex
	writes [][]Message
	err    error
	closed bool
}

func newMockDest() *mockDest { return &mockDest{} }

func (m *mockDest) Write(_ context.Context, msgs []Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Cloned because the pipeline is free to reuse its batch slice between
	// writes, which would otherwise rewrite this record of what happened.
	m.writes = append(m.writes, slices.Clone(msgs))

	return m.err
}

func (m *mockDest) Name() string { return "mock" }

func (m *mockDest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	return nil
}

func (m *mockDest) written() [][]Message {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Clone(m.writes)
}
