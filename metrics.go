// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "xmidt"
	subsystem = "syslog_bridge"
)

// metrics holds every instrument the service reports.  It is passed explicitly
// to the components that record into it, so nothing reaches for a global.
type metrics struct {
	RecordsConsumed       prometheus.Counter
	ForeignRecords        *prometheus.CounterVec
	MessagesDelivered     *prometheus.CounterVec
	WriteErrors           *prometheus.CounterVec
	UndeliverableMessages prometheus.Counter
	BatchMessages         prometheus.Histogram
	BatchAgeSeconds       prometheus.Histogram
	BatchesFlushed        *prometheus.CounterVec
	SecondsSinceLastWrite prometheus.Gauge
	ConsumerLag           *prometheus.GaugeVec
}

// newMetrics builds the metrics and registers them into reg.
//
// Taking a Registerer rather than creating a registry is deliberate: it lets
// the caller decide what the registry is, expose these metrics alongside its
// own, and wrap them if it needs to.  A program embedding two bridges can keep
// them apart by wrapping:
//
//	bridge.NewMetrics(prometheus.WrapRegistererWith(
//		prometheus.Labels{"bridge": "syslog"}, reg))
//
// This package deliberately does not register the Go or process collectors.
// Those describe the process, not the bridge, so exactly one thing in a process
// may register them, and that thing is the program.  A library that registered
// them would break any program that also registered them itself.
//
// An error means a duplicate or invalid metric definition, which is a
// programming error worth failing on.
func newMetrics(reg prometheus.Registerer) (*metrics, error) { // nolint: funlen // a declaration list
	m := &metrics{
		RecordsConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "records_consumed_total",
			Help: "The number of Kafka records consumed.",
		}),
		ForeignRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "foreign_records_total",
			Help: "Records skipped because they are not ours, by why.",
		}, []string{"reason"}),
		MessagesDelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "messages_delivered_total",
			Help: "Syslog messages written to the destination.",
		}, []string{"destination"}),
		WriteErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "write_errors_total",
			Help: "Destination write failures, by whether a retry could ever succeed.",
		}, []string{"class"}),
		UndeliverableMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "undeliverable_messages_total",
			Help: "Messages dropped because no retry could ever deliver them.",
		}),
		BatchMessages: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name:    "batch_messages",
			Help:    "Messages per batch at flush.",
			Buckets: []float64{1, 10, 100, 1000, 5000, 10000, 50000},
		}),
		BatchAgeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name:    "batch_age_seconds",
			Help:    "Age of a batch at flush, measured from its first message.",
			Buckets: []float64{0.1, 1, 5, 15, 30, 60, 300, 900},
		}),
		BatchesFlushed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "batches_flushed_total",
			Help: "Batches written, by which trigger closed them.",
		}, []string{"trigger"}),
		SecondsSinceLastWrite: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "seconds_since_last_write",
			Help: "Seconds since the last successful destination write.",
		}),
		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Subsystem: subsystem,
			Name: "consumer_lag",
			Help: "Records behind the high water mark, by partition.",
		}, []string{"partition"}),
	}

	for _, c := range m.collectors() {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.RecordsConsumed, m.ForeignRecords, m.MessagesDelivered,
		m.WriteErrors, m.UndeliverableMessages, m.BatchMessages,
		m.BatchAgeSeconds, m.BatchesFlushed, m.SecondsSinceLastWrite,
		m.ConsumerLag,
	}
}
