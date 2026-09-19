// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bridge

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Kafka fixture proving itself, with no bridge code involved.  Tagged
// because each of these needs a broker.

// Containers are started once per package, not once per test: a second ask
// must hand back the same broker rather than starting another.
func TestKafkaFixtureStartsOnceForThePackage(t *testing.T) {
	first := kafkaBrokers(t)
	second := kafkaBrokers(t)

	require.NotEmpty(t, first)
	assert.Equal(t, first, second)
}

// Two tests must never share a topic, or one test's records become another's.
func TestNewTopicIsUniquePerTest(t *testing.T) {
	a := newTopic(t, 1)
	b := newTopic(t, 1)

	assert.NotEqual(t, a, b)
}

// The fixture has to carry bytes, not text.  A broker or a client that helpfully
// normalized a payload would silently break the byte-identical promise that the
// whole path exists to keep, so the round trip is asserted on the awkward cases.
func TestKafkaFixtureRoundTripsValuesByteForByte(t *testing.T) {
	topic := newTopic(t, 1)

	payloads := [][]byte{
		[]byte(`kernel: apparmor="DENIED" operation="open" profile="/usr/bin/foo"`),
		[]byte("a message\nsplit over two lines"),
		{0x00, 0x01, 0x02, 0xff, 0x00},
		bytes.Repeat([]byte("x"), 4096),
		{},
	}

	produce(t, topic, payloads...)

	assert.Equal(t, payloads, consume(t, topic, newGroup(t), len(payloads)))
}
