// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

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
	prefix    = "ttl="
	maxDigits = 9
)

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

type Observer struct {
	hbasecop.UnimplementedRegionObserver

	logger   atomic.Pointer[slog.Logger]
	accepted atomic.Uint64
	rejected atomic.Uint64
}

func New() *Observer { return &Observer{} }

func (o *Observer) SetLogger(l *slog.Logger) { o.logger.Store(l) }

func (o *Observer) log() *slog.Logger {
	if l := o.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

func (o *Observer) Accepted() uint64 { return o.accepted.Load() }

func (o *Observer) Rejected() uint64 { return o.rejected.Load() }

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
					"ttl-validator: %s:%s - %w", family, qv.GetQualifier(), err)
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
