// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/virogg/go-hbase/internal/wire"
	"github.com/virogg/go-hbase/internal/wire/wirepb"
)

type funcEndpoint func(ctx context.Context, env *EndpointEnv, method string, payload []byte) ([]byte, error)

func (f funcEndpoint) Call(ctx context.Context, env *EndpointEnv, method string, payload []byte) ([]byte, error) {
	return f(ctx, env, method, payload)
}

func endpointInvokeFrame(t *testing.T, reqID uint64, method string, payload []byte) *wire.Message {
	t.Helper()
	b, err := proto.Marshal(&wirepb.EndpointInvoke{Method: method, Payload: payload})
	if err != nil {
		t.Fatalf("marshal EndpointInvoke: %v", err)
	}
	return &wire.Message{Type: wire.TypeEndpointInvoke, ReqID: reqID, Payload: b}
}

func TestDispatchEndpointReturnsResult(t *testing.T) {
	var gotMethod string
	d := &dispatcher{
		logger: slog.Default(),
		endpoint: funcEndpoint(func(_ context.Context, _ *EndpointEnv, method string, payload []byte) ([]byte, error) {
			gotMethod = method
			return append([]byte("ok:"), payload...), nil
		}),
	}

	resp := d.dispatchEndpoint(context.Background(), endpointInvokeFrame(t, 5, "sum", []byte("xy")))

	if resp.Type != wire.TypeEndpointResult || resp.ReqID != 5 {
		t.Fatalf("got %+v, want EndpointResult req_id=5", resp)
	}
	if gotMethod != "sum" {
		t.Errorf("method = %q, want sum", gotMethod)
	}
	var result wirepb.EndpointResult
	if err := proto.Unmarshal(resp.Payload, &result); err != nil {
		t.Fatalf("unmarshal EndpointResult: %v", err)
	}
	if string(result.GetPayload()) != "ok:xy" {
		t.Errorf("payload = %q, want ok:xy", result.GetPayload())
	}
}

func TestDispatchEndpointErrorBecomesErrorFrame(t *testing.T) {
	d := &dispatcher{
		logger: slog.Default(),
		endpoint: funcEndpoint(func(_ context.Context, _ *EndpointEnv, _ string, _ []byte) ([]byte, error) {
			return nil, errors.New("boom")
		}),
	}
	resp := d.dispatchEndpoint(context.Background(), endpointInvokeFrame(t, 1, "m", nil))
	if resp.Type != wire.TypeError {
		t.Fatalf("type = %v, want Error", resp.Type)
	}
	var werr wirepb.Error
	if err := proto.Unmarshal(resp.Payload, &werr); err != nil {
		t.Fatalf("unmarshal Error: %v", err)
	}
	if werr.GetCode() != errCodeEndpointFailed {
		t.Errorf("code = %d, want %d", werr.GetCode(), errCodeEndpointFailed)
	}
}

func TestDispatchEndpointPanicBecomesError(t *testing.T) {
	d := &dispatcher{
		logger: slog.Default(),
		endpoint: funcEndpoint(func(_ context.Context, _ *EndpointEnv, _ string, _ []byte) ([]byte, error) {
			panic("kaboom")
		}),
	}
	resp := d.dispatchEndpoint(context.Background(), endpointInvokeFrame(t, 2, "m", nil))
	if resp.Type != wire.TypeError {
		t.Fatalf("a panicking endpoint must yield an Error frame, got %v", resp.Type)
	}
}

func TestDispatchEndpointNilEndpointIsError(t *testing.T) {
	d := &dispatcher{logger: slog.Default()}
	resp := d.dispatchEndpoint(context.Background(), endpointInvokeFrame(t, 3, "m", nil))
	if resp.Type != wire.TypeError {
		t.Fatalf("type = %v, want Error when no endpoint registered", resp.Type)
	}
}

// TestDispatchEndpointPropagatesContextAndMethod pins the single-handler
// contract: the caller's context reaches the handler, and an arbitrary/unknown
// method name is passed through verbatim (routing by name is the user's job).
func TestDispatchEndpointPropagatesContextAndMethod(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "deadline-owner")

	var gotMethod string
	var gotCtxVal any
	d := &dispatcher{
		logger: slog.Default(),
		endpoint: funcEndpoint(func(c context.Context, _ *EndpointEnv, method string, _ []byte) ([]byte, error) {
			gotMethod = method
			gotCtxVal = c.Value(ctxKey{})
			return []byte("ok"), nil
		}),
	}

	resp := d.dispatchEndpoint(ctx, endpointInvokeFrame(t, 9, "totally.unknown.method", nil))
	if resp.Type != wire.TypeEndpointResult {
		t.Fatalf("type = %v, want EndpointResult", resp.Type)
	}
	if gotMethod != "totally.unknown.method" {
		t.Errorf("method = %q, want it passed through verbatim", gotMethod)
	}
	if gotCtxVal != "deadline-owner" {
		t.Errorf("ctx value = %v, want the caller's context propagated", gotCtxVal)
	}
}

// TestEncodeMinimalErrorRoundTrips verifies the hand-encoded fallback used when
// proto.Marshal of a wirepb.Error fails: it must still decode to the same Error
// so the client gets a prompt failure instead of a silent hang (E2GO-4).
func TestEncodeMinimalErrorRoundTrips(t *testing.T) {
	raw := encodeMinimalError(errCodeMarshalResponse, "boom detail")
	var werr wirepb.Error
	if err := proto.Unmarshal(raw, &werr); err != nil {
		t.Fatalf("hand-encoded Error must be valid protobuf: %v", err)
	}
	if werr.GetCode() != errCodeMarshalResponse {
		t.Errorf("code = %d, want %d", werr.GetCode(), errCodeMarshalResponse)
	}
	if werr.GetMessage() != "boom detail" {
		t.Errorf("message = %q, want %q", werr.GetMessage(), "boom detail")
	}
}
