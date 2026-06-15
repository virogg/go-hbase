// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

// Package hbasecoptest unit-tests RegionObserver implementations in process,
// without a cluster: it drives the production dispatcher and returns the
// decoded HookResult/error, exercising env decode, panic recovery and result
// mapping the same way the Java side would.
package hbasecoptest

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

// Env is the table/region metadata a hook receives as ObserverEnv. Table may be
// "namespace:qualifier" or a bare qualifier.
type Env struct {
	Table  string
	Region string
}

// Context builds the HookContext envelope carrying env; set it as a request's
// Ctx before calling Invoke.
func Context(env Env) *hbasecop.HookContext {
	hc := &hbasecop.HookContext{RegionName: []byte(env.Region)}
	if env.Table != "" {
		ns, q := "", env.Table
		if i := strings.IndexByte(env.Table, ':'); i >= 0 {
			ns, q = env.Table[:i], env.Table[i+1:]
		}
		hc.TableName = &hbasecop.TableName{Namespace: []byte(ns), Qualifier: []byte(q)}
	}
	return hc
}

// Invoke drives one hook. The caller sets req.Ctx (e.g. via Context); for the
// envelope hooks req is the *XxxRequest itself.
func Invoke(obs hbasecop.RegionObserver, hookID hbasecop.HookID, req proto.Message) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hookID, req)
}

// PrePut drives PrePut with a mutation, wrapping it in the request envelope.
func PrePut(obs hbasecop.RegionObserver, env Env, mut *hbasecop.MutationProto) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hbasecop.HookIDPrePut, &hbasecop.PrePutRequest{Ctx: Context(env), Mutation: mut})
}

// PostPut drives PostPut with a mutation.
func PostPut(obs hbasecop.RegionObserver, env Env, mut *hbasecop.MutationProto) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hbasecop.HookIDPostPut, &hbasecop.PostPutRequest{Ctx: Context(env), Mutation: mut})
}
