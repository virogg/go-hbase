// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package tools pins build-time CLI dependencies so `go mod tidy` keeps them in
// go.sum even though no production code imports them.
//
//go:build tools

package tools
