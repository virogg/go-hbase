// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import "log/slog"

type ObserverEnv struct {
	TableName  string
	RegionName string
	RegionID   uint32

	logger *slog.Logger
	hook   string
	reqID  uint64
}

func (e ObserverEnv) Logger() *slog.Logger {
	if e.logger == nil {
		return slog.Default()
	}
	return e.logger.With("hook", e.hook, "req_id", e.reqID, "table", e.TableName, "region", e.RegionName)
}
