// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bridge

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The Kafka fixture.  Behind the `integration` build tag so that the unit
// suite never waits on a container: `go test ./...` must stay fast enough to
// run on every save, which is what the red-green loop depends on.  The
// Makefile is the interface -- nobody should need to remember the tag.

const kafkaImage = "confluentinc/confluent-local:7.8.0"

// dockerProbe asks once whether a container runtime is reachable.  Once,
// because the health check has a timeout and there is no reason to pay it per
// test.
var dockerProbe = sync.OnceValue(func() error {
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return provider.Health(ctx)
})

// requireDocker skips t under -short, or when no container runtime is
// reachable.
func requireDocker(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping the container tests under -short")
	}

	if reason := dockerSkipReason(dockerProbe()); reason != "" {
		t.Skip(reason)
	}
}

// -----------------------------------------------------------------------------
// The Kafka fixture
// -----------------------------------------------------------------------------

var (
	kafkaOnce    sync.Once
	kafkaCtr     *kafka.KafkaContainer
	kafkaAddrs   []string
	kafkaStartup error
)

// TestMain tears the broker down.  It deliberately does not start one: a run
// that touches no integration test -- the common case while writing pure logic
// -- should not wait on a container it never uses.
func TestMain(m *testing.M) {
	code := m.Run()

	if kafkaCtr != nil {
		if err := testcontainers.TerminateContainer(kafkaCtr); err != nil {
			fmt.Fprintln(os.Stderr, "terminating the kafka fixture:", err)
		}
	}

	os.Exit(code)
}

// kafkaBrokers returns the bootstrap addresses of this package's broker,
// starting it the first time it is asked for.  One container serves the whole
// package; tests keep out of each other's way with their own topics and groups
// rather than their own brokers.
func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	requireDocker(t)

	kafkaOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		kafkaCtr, kafkaStartup = kafka.Run(ctx, kafkaImage)
		if kafkaStartup != nil {
			return
		}

		kafkaAddrs, kafkaStartup = kafkaCtr.Brokers(ctx)
	})

	require.NoError(t, kafkaStartup, "starting the kafka fixture")
	require.NotEmpty(t, kafkaAddrs)

	return kafkaAddrs
}

// nameSeq keeps two topics or groups made by one test apart.
var nameSeq atomic.Int64

// safeName turns a test name into something Kafka accepts, which is only
// letters, digits, dot, underscore and dash -- subtest names carry slashes and
// spaces that would otherwise be rejected at create time.
func safeName(t *testing.T) string {
	t.Helper()

	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '_', r == '-':
			return r
		default:
			return '-'
		}
	}, t.Name())

	return fmt.Sprintf("%s-%d", name, nameSeq.Add(1))
}

// newTopic creates a topic named for t, so two tests never share one.
func newTopic(t *testing.T, partitions int32) string {
	t.Helper()

	name := safeName(t)

	cl := newClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	resp, err := kadm.NewClient(cl).CreateTopic(ctx, partitions, 1, nil, name)
	require.NoError(t, err, "creating topic %s", name)
	require.NoError(t, resp.Err, "creating topic %s", name)

	return name
}

// newGroup returns a consumer group id unique to t.  A group carries committed
// offsets, so reusing one across tests makes a test's result depend on which
// tests ran before it.
func newGroup(t *testing.T) string {
	t.Helper()

	return safeName(t)
}

// newClient dials the fixture and closes when t finishes.
func newClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()

	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafkaBrokers(t)...)}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	return cl
}

// produce publishes each value to topic and waits for the broker to acknowledge
// them, so a test that produces then consumes cannot race its own setup.
func produce(t *testing.T, topic string, values ...[]byte) {
	t.Helper()

	recs := make([]*kgo.Record, 0, len(values))
	for _, v := range values {
		recs = append(recs, &kgo.Record{Topic: topic, Value: v})
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	require.NoError(t, newClient(t).ProduceSync(ctx, recs...).FirstErr())
}

// consume reads n record values from topic under group, from the start.
func consume(t *testing.T, topic, group string, n int) [][]byte {
	t.Helper()

	cl := newClient(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	values := make([][]byte, 0, n)
	for _, r := range pollRecords(t, cl, n) {
		values = append(values, r.Value)
	}

	return values
}

// pollRecords gathers exactly n records, failing rather than hanging if they do
// not arrive.  Polling in a loop is necessary: one fetch is not obliged to
// return everything that is there.
func pollRecords(t *testing.T, cl *kgo.Client, n int) []*kgo.Record {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	got := make([]*kgo.Record, 0, n)
	for len(got) < n {
		fetches := cl.PollRecords(ctx, n-len(got))

		for _, e := range fetches.Errors() {
			require.NoError(t, e.Err, "fetching %s partition %d", e.Topic, e.Partition)
		}
		require.NoError(t, ctx.Err(), "wanted %d records, got %d", n, len(got))

		fetches.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}

	return got
}

// rebalanceLog records what the consumer group did to a member.
//
// An eviction shows up here as a revoke, which is precisely what the heartbeat
// verification must not see while the member is paused.
type rebalanceLog struct {
	mu    sync.Mutex
	held  map[string][]int32
	count int
}

func (r *rebalanceLog) assigned(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.held == nil {
		r.held = make(map[string][]int32)
	}

	for topic, parts := range m {
		r.held[topic] = append(r.held[topic], parts...)
	}
}

func (r *rebalanceLog) revoked(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.count++

	for topic, parts := range m {
		r.held[topic] = slices.DeleteFunc(r.held[topic], func(p int32) bool {
			return slices.Contains(parts, p)
		})
	}
}

func (r *rebalanceLog) revokes() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.count
}

// owned reports the partitions of topic this member currently holds, sorted so
// an assertion does not depend on assignment order.
func (r *rebalanceLog) owned(topic string) []int32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Sorted(slices.Values(r.held[topic]))
}

// -----------------------------------------------------------------------------
// The syslog sink
// -----------------------------------------------------------------------------
