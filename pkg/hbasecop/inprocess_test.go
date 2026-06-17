// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestInvokeRegion(t *testing.T) {
	tests := []struct {
		name    string
		obs     RegionObserver
		hookID  HookID
		wantErr string // substring; "" = no error
		bypass  bool
	}{
		{
			name: "bypass",
			obs: NewRegion().OnPrePut(func(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
				return HookResult{Bypass: true}, nil
			}),
			hookID: HookIDPrePut,
			bypass: true,
		},
		{
			name: "observer error",
			obs: NewRegion().OnPrePut(func(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
				return HookResult{}, errors.New("denied")
			}),
			hookID:  HookIDPrePut,
			wantErr: "denied",
		},
		{
			name: "panic recovered",
			obs: NewRegion().OnPrePut(func(context.Context, ObserverEnv, *MutationProto) (HookResult, error) {
				panic("boom")
			}),
			hookID:  HookIDPrePut,
			wantErr: "panic",
		},
		{
			name:    "unknown hook",
			obs:     NewRegion(),
			hookID:  HookID(0),
			wantErr: "unknown hook",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := InvokeRegion(tc.obs, tc.hookID, &PrePutRequest{Mutation: &MutationProto{Row: []byte("r")}})
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
			if res.Bypass != tc.bypass {
				t.Fatalf("bypass = %v, want %v", res.Bypass, tc.bypass)
			}
		})
	}
}
