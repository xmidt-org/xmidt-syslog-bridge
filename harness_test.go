// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests prove the doubles themselves, with no bridge code and no
// container involved.  They exist because every slice from WI-03 on rests on
// these doubles, so a double that lies would make a whole suite green for the
// wrong reason.  The Kafka fixture's own tests need a broker and are in
// harness_integration_test.go.

// -----------------------------------------------------------------------------
// requireDocker
// -----------------------------------------------------------------------------

// A skip must name why.  A suite that quietly skipped its integration tests
// looks exactly like one that ran them, which is the failure this guards.
func TestDockerSkipReasonNamesTheCause(t *testing.T) {
	assert.Empty(t, dockerSkipReason(nil),
		"a reachable runtime must not produce a skip")

	reason := dockerSkipReason(errors.New("cannot connect to the docker daemon"))

	assert.Contains(t, reason, "docker unavailable")
	assert.Contains(t, reason, "cannot connect to the docker daemon")
}

// -----------------------------------------------------------------------------
// newSyslogSink
// -----------------------------------------------------------------------------

// The sink is what WI-03 asserts byte-identical delivery against, so it must
// record each datagram whole and in order -- including the two payloads that
// break a sink written for text.
func TestSyslogSinkRecordsEachDatagramInOrder(t *testing.T) {
	path, received := newSyslogSink(t)

	sent := [][]byte{
		[]byte("first"),
		[]byte("second\nwith an embedded newline"),
		{'a', 0x00, 'b'},
		bytes.Repeat([]byte("y"), 8192),
	}

	conn, err := net.Dial("unixgram", path)
	require.NoError(t, err)
	defer conn.Close() // nolint: errcheck

	for _, p := range sent {
		_, err := conn.Write(p)
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool { return len(received()) == len(sent) },
		2*time.Second, 5*time.Millisecond)
	assert.Equal(t, sent, received())
}

// -----------------------------------------------------------------------------
// mockSource
// -----------------------------------------------------------------------------

func TestMockSourceServesOneBatchPerPoll(t *testing.T) {
	m := newMockSource(
		[]Record{{Partition: 0, Offset: 0, Value: []byte("a")}},
		[]Record{{Partition: 0, Offset: 1, Value: []byte("b")}, {Partition: 0, Offset: 2, Value: []byte("c")}},
	)

	first, err := m.Poll(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []Record{{Offset: 0, Value: []byte("a")}}, first)

	second, err := m.Poll(t.Context())
	require.NoError(t, err)
	require.Len(t, second, 2)
	assert.Equal(t, int64(2), second[1].Offset)
}

// When the canned records run out the mock blocks until its context is done,
// which is what a real consumer on an idle topic does.  A mock that reported
// exhaustion instead would let a pipeline pass a shutdown test that it would
// fail against Kafka.
func TestMockSourceBlocksOnceItsRecordsRunOut(t *testing.T) {
	m := newMockSource()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	recs, err := m.Poll(ctx)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, recs)
}

// The drain knob is for tests that want the loop to finish on its own rather
// than be canceled.  An empty batch is "nothing yet", not an error.
func TestMockSourceDrainsWithoutBlocking(t *testing.T) {
	m := newMockSource()
	m.drain = true

	recs, err := m.Poll(t.Context())

	assert.NoError(t, err)
	assert.Empty(t, recs)
}

// A paused source serves nothing and wakes on Resume.  This is what lets WI-05
// assert that pausing actually stops delivery rather than only setting a flag.
func TestMockSourcePauseStopsDeliveryUntilResume(t *testing.T) {
	m := newMockSource([]Record{{Offset: 7, Value: []byte("held")}})
	m.Pause()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	_, err := m.Poll(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded, "a paused source must serve nothing")

	go m.Resume()

	recs, err := m.Poll(t.Context())
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, int64(7), recs[0].Offset)
}

// Both are idempotent per the Source contract.
func TestMockSourcePauseAndResumeAreIdempotent(t *testing.T) {
	m := newMockSource()

	m.Pause()
	m.Pause()
	assert.True(t, m.isPaused())

	m.Resume()
	m.Resume()
	assert.False(t, m.isPaused())
}

func TestMockSourceRecordsWhatWasCommitted(t *testing.T) {
	m := newMockSource()
	recs := []Record{{Offset: 1}, {Offset: 2}}

	require.NoError(t, m.Commit(t.Context(), recs))
	require.NoError(t, m.Commit(t.Context(), []Record{{Offset: 3}}))

	assert.Equal(t, []Record{{Offset: 1}, {Offset: 2}, {Offset: 3}}, m.commits())
}

func TestMockSourceRecordsThatItWasClosed(t *testing.T) {
	m := newMockSource()

	require.NoError(t, m.Close())

	assert.True(t, m.isClosed())
}

// -----------------------------------------------------------------------------
// mockDest
// -----------------------------------------------------------------------------

// Each Write is recorded as its own batch, because how messages are grouped is
// exactly what WI-06 asserts.
func TestMockDestRecordsEachWriteAsItsOwnBatch(t *testing.T) {
	d := newMockDest()

	require.NoError(t, d.Write(t.Context(), []Message{{Offset: 0}, {Offset: 1}}))
	require.NoError(t, d.Write(t.Context(), []Message{{Offset: 2}}))

	got := d.written()
	require.Len(t, got, 2)
	assert.Len(t, got[0], 2)
	require.Len(t, got[1], 1)
	assert.Equal(t, int64(2), got[1][0].Offset)
}

// The mock copies what it is handed.  A pipeline that reuses its slice between
// writes would otherwise rewrite this test's evidence underneath it, and the
// assertion would describe the last batch twice.
func TestMockDestCopiesTheBatchItIsGiven(t *testing.T) {
	d := newMockDest()
	batch := []Message{{Offset: 1, Payload: []byte("original")}}

	require.NoError(t, d.Write(t.Context(), batch))
	batch[0] = Message{Offset: 99, Payload: []byte("reused")}

	got := d.written()
	require.Len(t, got, 1)
	require.Len(t, got[0], 1)
	assert.Equal(t, int64(1), got[0][0].Offset)
}

func TestMockDestReturnsTheErrorItWasGiven(t *testing.T) {
	transient := errors.New("connection refused")
	d := newMockDest()
	d.err = transient

	err := d.Write(t.Context(), []Message{{Offset: 0}})

	assert.ErrorIs(t, err, transient)
	assert.Len(t, d.written(), 1, "a failed write is still an attempt worth recording")
}

// Wrapping in Permanent is how a test drives the drop path in WI-05.
func TestMockDestCanFailPermanently(t *testing.T) {
	d := newMockDest()
	d.err = Permanent(errors.New("message too long"))

	var pe *PermanentError
	assert.True(t, errors.As(d.Write(t.Context(), nil), &pe))
}

func TestMockDestIsNamedForItsMetricLabel(t *testing.T) {
	assert.Equal(t, "mock", newMockDest().Name())
	assert.True(t, newMockDest().Close() == nil)
}
