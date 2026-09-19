// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This file answers the one question the design had not verified, and then
// keeps answering it.
//
// ADR 0006 has the bridge pause its fetch for the length of a destination
// outage rather than failing.  That is only survivable if the paused member
// keeps its place in the consumer group: if the broker evicted it instead,
// every outage would end in a rebalance storm, and the backpressure design
// would need rethinking rather than patching.
//
// The test stays after the answer is recorded in docs/design.md section 3.
// franz-go is on dependabot here, and a heartbeat that stopped firing while
// paused would be an invisible change in a minor version -- visible only as
// production rebalancing during an outage, which is the worst possible place
// to discover it.

// The broker's group.min.session.timeout.ms is 6s by default, so that is the
// shortest session this test can ask for.  The pause is three sessions long:
// if heartbeats stopped with the fetch, the member is evicted well inside it.
const (
	heartbeatSession = 6 * time.Second
	heartbeatPause   = 3 * heartbeatSession
)

func TestPausedMemberKeepsItsGroupAssignment(t *testing.T) {
	brokers := kafkaBrokers(t)
	topic := newTopic(t, 1)
	group := newGroup(t)

	var rebalances rebalanceLog

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(heartbeatSession),
		kgo.HeartbeatInterval(heartbeatSession/3),
		kgo.OnPartitionsAssigned(rebalances.assigned),
		kgo.OnPartitionsRevoked(rebalances.revoked),
	)
	require.NoError(t, err)
	defer cl.Close()

	// Join the group and take delivery of one record, so the member is fully
	// assigned before anything is measured.
	produce(t, topic, []byte("before the pause"))
	first := pollRecords(t, cl, 1)
	require.Len(t, first, 1)
	require.Equal(t, "before the pause", string(first[0].Value))
	require.NoError(t, cl.CommitRecords(t.Context(), first...))

	_, generation := cl.GroupMetadata()
	revokesBefore := rebalances.revokes()

	// Pause, as the bridge does when its destination is failing.
	require.Equal(t, []string{topic}, cl.PauseFetchTopics(topic))

	// Records keep arriving on the topic during the outage; the point of
	// pausing is that they stay on the broker rather than in our memory.
	produce(t, topic, []byte("during the pause"))

	time.Sleep(heartbeatPause)

	// The three assertions that matter, in the order they would fail.
	assert.Equal(t, revokesBefore, rebalances.revokes(),
		"the member was revoked while paused: franz-go stopped heartbeating, and ADR 0006 needs revisiting")

	_, generationAfter := cl.GroupMetadata()
	assert.Equal(t, generation, generationAfter,
		"the group generation advanced while paused, which means a rebalance happened")

	assert.Equal(t, []int32{0}, rebalances.owned(topic),
		"the member no longer holds the partition it was assigned")

	// Nothing may have been delivered while paused, or pausing bought nothing.
	idle, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	assert.Zero(t, cl.PollRecords(idle, 1).NumRecords(),
		"a paused member must fetch nothing")
	cancel()

	// And it resumes from where it stopped rather than from the start or the
	// end of the partition.
	cl.ResumeFetchTopics(topic)

	after := pollRecords(t, cl, 1)
	require.Len(t, after, 1)
	assert.Equal(t, "during the pause", string(after[0].Value))
	assert.Equal(t, int64(1), after[0].Offset,
		"delivery resumed somewhere other than where it stopped")
}
