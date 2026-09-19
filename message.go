// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

// Record is one item from the Event Source, before screening.  It carries only
// what the bridge needs, so no Kafka type reaches the rest of the code.
type Record struct {
	Partition int32
	Offset    int64
	Value     []byte // msgpack-encoded WRP
}

// Message is one Syslog Message that passed screening, with enough of its
// origin to acknowledge it and to describe it in S3 object metadata.
type Message struct {
	Payload   []byte // the syslog bytes, already gunzipped, never altered
	Partition int32
	Offset    int64
}

// PermanentError marks a write that no retry could ever complete.  The retry
// loop drops these; everything else it retries forever.  See ADR 0004.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as permanently undeliverable.
//
// Callers classify with errors.As and never by matching a string, because the
// error has passed through several layers of wrapping by the time the retry
// loop sees it:
//
//	var pe *PermanentError
//	if errors.As(err, &pe) { /* drop, count, log, commit */ }
func Permanent(err error) error { return &PermanentError{Err: err} }
