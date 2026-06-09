// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package ttl implements the pre-hook validation RegionObserver (T73). It
// demonstrates the strict failure policy: PrePut inspects every cell value
// and rejects the whole Put unless each value declares a TTL envelope.
// Under the default strict policy for pre-hooks, a rejection becomes an
// IOException at the HBase client and the write is aborted.
//
// The demo envelope convention is a textual prefix:
//
//	ttl=<seconds>;<payload>
//
// e.g. "ttl=3600;{...}" — at least one digit, seconds > 0, terminated by
// ';'. Real deployments would carry the TTL in a structured codec; the
// point here is in-database validation written in Go.
package ttl

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

const (
	prefix = "ttl="
	// maxDigits caps the seconds field at 9 digits (≈31.7 years). Besides
	// bounding the parse, this keeps seconds*time.Second far inside
	// time.Duration's int64 range (which overflows at ~292 years).
	maxDigits = 9
)

// Validate checks that value carries the ttl=<seconds>; envelope and
// returns the declared TTL. The error is descriptive but never echoes the
// value bytes (SPEC §8: cell values must not reach logs or client-visible
// errors).
func Validate(value []byte) (time.Duration, error) {
	if len(value) < len(prefix)+2 { // "ttl=" + at least one digit + ';'
		return 0, fmt.Errorf("value lacks the %q TTL envelope", prefix)
	}
	if string(value[:len(prefix)]) != prefix {
		return 0, fmt.Errorf("value does not start with %q", prefix)
	}
	rest := value[len(prefix):]
	var seconds uint64
	i := 0
	for ; i < len(rest) && i < maxDigits+1; i++ {
		c := rest[i]
		if c == ';' {
			break
		}
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("TTL seconds must be digits terminated by ';'")
		}
		seconds = seconds*10 + uint64(c-'0')
	}
	if i == 0 {
		return 0, fmt.Errorf("TTL envelope has no digits")
	}
	if i > maxDigits {
		return 0, fmt.Errorf("TTL seconds field longer than %d digits", maxDigits)
	}
	if i >= len(rest) || rest[i] != ';' {
		return 0, fmt.Errorf("TTL envelope not terminated by ';'")
	}
	if seconds == 0 {
		return 0, fmt.Errorf("TTL must be > 0 seconds")
	}
	return time.Duration(seconds) * time.Second, nil
}

// Observer rejects Puts whose cell values lack the TTL envelope. All other
// RegionObserver methods inherit no-op behaviour. PrePut runs under the
// strict policy by default, so a rejection aborts the client's write with
// an IOException.
type Observer struct {
	hbasecop.UnimplementedRegionObserver

	logger   atomic.Pointer[slog.Logger]
	accepted atomic.Uint64
	rejected atomic.Uint64
}

// New constructs a TTL-validating Observer logging via slog.Default().
func New() *Observer { return &Observer{} }

// SetLogger overrides the slog.Logger decisions are logged to. Nil resets
// to slog.Default(). Safe for concurrent use.
func (o *Observer) SetLogger(l *slog.Logger) { o.logger.Store(l) }

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// Accepted and Rejected expose the decision counters (used by tests).
func (o *Observer) Accepted() uint64 { return o.accepted.Load() }

// Rejected reports how many Puts were rejected so far.
func (o *Observer) Rejected() uint64 { return o.rejected.Load() }

// PrePut validates every cell value in the mutation. The first violation
// rejects the whole Put; the returned error reaches the HBase client as an
// IOException under the default strict pre-hook policy. Error text names
// column coordinates (family/qualifier are schema, not payload) but never
// the value bytes.
func (o *Observer) PrePut(
	_ context.Context,
	env hbasecop.ObserverEnv,
	mut *hbasecop.MutationProto,
) (hbasecop.HookResult, error) {
	for _, cv := range mut.GetColumnValue() {
		family := string(cv.GetFamily())
		for _, qv := range cv.GetQualifierValue() {
			if _, err := Validate(qv.GetValue()); err != nil {
				n := o.rejected.Add(1)
				o.log().Info("ttl-validator: rejected",
					"table", env.TableName,
					"region", env.RegionName,
					"family", family,
					"qualifier", string(qv.GetQualifier()),
					"reason", err.Error(),
					"rejected_total", n,
				)
				return hbasecop.HookResult{}, fmt.Errorf(
					"ttl-validator: %s:%s — %w", family, qv.GetQualifier(), err)
			}
		}
	}
	n := o.accepted.Add(1)
	o.log().Info("ttl-validator: accepted",
		"table", env.TableName,
		"region", env.RegionName,
		"accepted_total", n,
	)
	return hbasecop.HookResult{}, nil
}
