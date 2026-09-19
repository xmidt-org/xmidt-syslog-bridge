// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"errors"
	"fmt"
	"text/template"
	"time"
)

// ErrInvalidConfig is returned when the configuration cannot describe a
// runnable instance.
var ErrInvalidConfig = errors.New("invalid configuration")

// Config is everything the bridge needs.  The program that embeds it decides
// where these values come from; this package never learns what a YAML key is
// called.
type Config struct {
	Servers Servers

	// Instance names this instance.  It defaults to the hostname.  It must be
	// unique across instances writing to one bucket, because it is what
	// separates their object keys.
	Instance string

	Kafka  Kafka
	Filter Filter

	// Exactly one of Syslog or S3 must be present.  Zero or two is a startup
	// error; see Validate.
	Syslog *Syslog
	S3     *S3
}

// Kafka describes the event source.  The key names mirror what the wrpkafka
// library uses on the producer side so operators see one shape across the
// platform.  The type is deliberately not shared with that library, which also
// carries producer-only keys that are meaningless here.
type Kafka struct {
	Brokers []string
	Topic   string
	Group   string
	TLS     *TLS
	SASL    *SASL
}

type TLS struct {
	Enabled bool
	CAFile  string
}

type SASL struct {
	Mechanism string
	Username  string
	Password  string
}

// Filter decides which records are ours.  Event is matched against the parsed
// WRP locator's authority, never as a string prefix.
type Filter struct {
	Event string
}

// Syslog is the local syslog destination.  Network and Address take the
// standard library's dial shape; only "unixgram" is supported today, which
// keeps adding a transport later an enum extension rather than a redesign.
type Syslog struct {
	Network string
	Address string
}

// S3 is the object storage destination.
type S3 struct {
	Bucket   string
	Region   string
	Endpoint string
	GZIP     bool

	// KeyTemplate is rendered per batch.  A malformed template fails at
	// startup, not on first flush.
	KeyTemplate string

	Batch Batch
}

// Batch bounds how much is held in memory per Kafka partition before an object
// is written.  A batch closes on whichever of these is reached first.
type Batch struct {
	MaxCount        int
	MaxBatchTimeout time.Duration
	MaxBytes        int64
}

// Validate enforces the rules that the struct tags cannot express.  It is
// deliberately strict: every one of these failures is far cheaper to find at
// startup than in production.
func (c Config) Validate() error {
	switch {
	case len(c.Kafka.Brokers) == 0:
		return fmt.Errorf("%w: kafka.brokers is required", ErrInvalidConfig)
	case c.Kafka.Topic == "":
		return fmt.Errorf("%w: kafka.topic is required", ErrInvalidConfig)
	case c.Kafka.Group == "":
		return fmt.Errorf("%w: kafka.group is required", ErrInvalidConfig)
	case c.Filter.Event == "":
		return fmt.Errorf("%w: filter.event is required", ErrInvalidConfig)
	}

	switch {
	case c.Syslog == nil && c.S3 == nil:
		return fmt.Errorf("%w: no destination configured, set exactly one of `syslog` or `s3`",
			ErrInvalidConfig)
	case c.Syslog != nil && c.S3 != nil:
		return fmt.Errorf("%w: both `syslog` and `s3` are configured, set exactly one; "+
			"to feed both, run two instances under different consumer groups",
			ErrInvalidConfig)
	}

	if c.Syslog != nil {
		if c.Syslog.Network != "unixgram" {
			return fmt.Errorf("%w: syslog.network must be `unixgram`, got `%s`",
				ErrInvalidConfig, c.Syslog.Network)
		}
		if c.Syslog.Address == "" {
			return fmt.Errorf("%w: syslog.address is required", ErrInvalidConfig)
		}
	}

	if c.S3 != nil {
		if c.S3.Bucket == "" {
			return fmt.Errorf("%w: s3.bucket is required", ErrInvalidConfig)
		}
		if c.S3.KeyTemplate == "" {
			return fmt.Errorf("%w: s3.key_template is required", ErrInvalidConfig)
		}
		if _, err := template.New("key").Parse(c.S3.KeyTemplate); err != nil {
			return fmt.Errorf("%w: s3.key_template does not parse: %s", ErrInvalidConfig, err)
		}
		if c.S3.Batch.MaxCount < 1 {
			return fmt.Errorf("%w: s3.batch.max_count must be at least 1", ErrInvalidConfig)
		}
		if c.S3.Batch.MaxBatchTimeout <= 0 {
			return fmt.Errorf("%w: s3.batch.max_batch_timeout must be positive", ErrInvalidConfig)
		}
		if c.S3.Batch.MaxBytes < 1 {
			return fmt.Errorf("%w: s3.batch.max_bytes must be at least 1", ErrInvalidConfig)
		}
	}

	return nil
}

// Destination names the active destination.
func (c Config) Destination() string {
	if c.Syslog != nil {
		return "syslog"
	}
	return "s3"
}
