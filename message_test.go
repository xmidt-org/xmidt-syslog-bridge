// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package bridge

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Permanent must preserve the error it wraps, because the log line that
// reports a dropped message is the only record that it existed.
func TestPermanentPreservesTheErrorItWraps(t *testing.T) {
	err := Permanent(syscall.EMSGSIZE)

	assert.ErrorIs(t, err, syscall.EMSGSIZE)
	assert.Equal(t, syscall.EMSGSIZE.Error(), err.Error())
}

// Classification happens after the error has passed through the layers between
// the syscall and the retry loop, so errors.As must find it through wrapping.
// This is why callers never match on a string.
func TestPermanentIsFoundThroughWrapping(t *testing.T) {
	err := fmt.Errorf("writing datagram to /dev/log: %w", Permanent(syscall.EMSGSIZE))

	var pe *PermanentError
	require.True(t, errors.As(err, &pe), "the retry loop must see through the wrapping")
	assert.ErrorIs(t, pe, syscall.EMSGSIZE)
}

// The failures that must be retried forever must NOT look permanent.  Getting
// this backwards drops messages that a retry would have delivered.
func TestTransientErrorsAreNotPermanent(t *testing.T) {
	for _, err := range []error{syscall.ECONNREFUSED, syscall.EAGAIN, errors.New("s3: 503")} {
		t.Run(err.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("writing: %w", err)

			var pe *PermanentError
			assert.False(t, errors.As(wrapped, &pe))
		})
	}
}
