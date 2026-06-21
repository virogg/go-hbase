// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecoptest

import (
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/pkg/hbasecop"
)

type Env struct {
	Table  string
	Region string
}

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

func Invoke(obs hbasecop.RegionObserver, hookID hbasecop.HookID, req proto.Message) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hookID, req)
}

func PrePut(obs hbasecop.RegionObserver, env Env, mut *hbasecop.MutationProto) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hbasecop.HookIDPrePut, &hbasecop.PrePutRequest{Ctx: Context(env), Mutation: mut})
}

func PostPut(obs hbasecop.RegionObserver, env Env, mut *hbasecop.MutationProto) (hbasecop.HookResult, error) {
	return hbasecop.InvokeRegion(obs, hbasecop.HookIDPostPut, &hbasecop.PostPutRequest{Ctx: Context(env), Mutation: mut})
}
