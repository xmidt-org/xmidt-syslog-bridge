// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func validS3() *S3 {
	return &S3{
		Bucket:      "apparmor-archive",
		Region:      "us-east-1",
		KeyTemplate: `{{.FlushTime.UTC.Format "2006/01/02"}}/{{.Instance}}{{.Ext}}`,
		Batch: Batch{
			MaxCount:        10000,
			MaxBatchTimeout: 60 * time.Second,
			MaxBytes:        64 << 20,
		},
	}
}

func validSyslog() *Syslog {
	return &Syslog{Network: "unixgram", Address: "/dev/log"}
}

// base is a configuration that is valid apart from its destination, so each
// test can vary only the thing it is about.
func base() Config {
	return Config{
		Kafka: Kafka{
			Brokers: []string{"localhost:9092"},
			Topic:   "apparmor-alerts",
			Group:   "xmidt-syslog-bridge",
		},
		Filter: Filter{Event: "apparmor"},
	}
}

func with(f func(*Config)) Config {
	c := base()
	f(&c)
	return c
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Config
		expectErr   bool
		errContains string
	}{
		{
			name: "syslog destination is enough",
			cfg:  with(func(c *Config) { c.Syslog = validSyslog() }),
		}, {
			name: "s3 destination is enough",
			cfg:  with(func(c *Config) { c.S3 = validS3() }),
		}, {
			name:        "no destination at all",
			cfg:         base(),
			expectErr:   true,
			errContains: "no destination configured",
		}, {
			name:        "both destinations",
			cfg:         with(func(c *Config) { c.Syslog, c.S3 = validSyslog(), validS3() }),
			expectErr:   true,
			errContains: "set exactly one",
		}, {
			// Only unixgram is supported today; udp and tcp cannot honor the
			// delivery guarantee.  See ADR 0003.
			name:        "network syslog transport is refused",
			cfg:         with(func(c *Config) { c.Syslog = &Syslog{Network: "udp", Address: "1.2.3.4:514"} }),
			expectErr:   true,
			errContains: "must be `unixgram`",
		}, {
			name:        "syslog without an address",
			cfg:         with(func(c *Config) { c.Syslog = &Syslog{Network: "unixgram"} }),
			expectErr:   true,
			errContains: "syslog.address is required",
		}, {
			name: "s3 without a bucket",
			cfg: with(func(c *Config) {
				s := validS3()
				s.Bucket = ""
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "s3.bucket is required",
		}, {
			// A malformed template must fail at startup, not on first flush,
			// which could be a minute into the run.
			name: "malformed key template",
			cfg: with(func(c *Config) {
				s := validS3()
				s.KeyTemplate = "{{.FlushTime"
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "does not parse",
		}, {
			name: "missing key template",
			cfg: with(func(c *Config) {
				s := validS3()
				s.KeyTemplate = ""
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "s3.key_template is required",
		}, {
			// Without a byte bound, a burst of large messages exhausts memory
			// and, with no spool, becomes a restart loop.
			name: "batch without a byte bound",
			cfg: with(func(c *Config) {
				s := validS3()
				s.Batch.MaxBytes = 0
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "max_bytes must be at least 1",
		}, {
			name: "batch without a timeout",
			cfg: with(func(c *Config) {
				s := validS3()
				s.Batch.MaxBatchTimeout = 0
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "max_batch_timeout must be positive",
		}, {
			// max_count of 1 is how one object per message is configured;
			// there is no separate mode for it.
			name: "a count of one is valid",
			cfg: with(func(c *Config) {
				s := validS3()
				s.Batch.MaxCount = 1
				c.S3 = s
			}),
		}, {
			name: "a count of zero is not",
			cfg: with(func(c *Config) {
				s := validS3()
				s.Batch.MaxCount = 0
				c.S3 = s
			}),
			expectErr:   true,
			errContains: "max_count must be at least 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()

			if !tc.expectErr {
				assert.NoError(t, err)
				return
			}

			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestConfigValidateRequiresSource(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*Config)
		errContains string
	}{
		{
			name:        "no brokers",
			mutate:      func(c *Config) { c.Kafka.Brokers = nil },
			errContains: "kafka.brokers is required",
		}, {
			name:        "no topic",
			mutate:      func(c *Config) { c.Kafka.Topic = "" },
			errContains: "kafka.topic is required",
		}, {
			name:        "no consumer group",
			mutate:      func(c *Config) { c.Kafka.Group = "" },
			errContains: "kafka.group is required",
		}, {
			// Without a filter event nothing would be recognized as ours, so
			// the service would silently discard the whole topic.
			name:        "no filter event",
			mutate:      func(c *Config) { c.Filter.Event = "" },
			errContains: "filter.event is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := with(func(c *Config) {
				c.Syslog = validSyslog()
				tc.mutate(c)
			})

			err := cfg.Validate()

			assert.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}
