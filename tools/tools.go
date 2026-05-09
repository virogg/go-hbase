// Package tools pins build-time CLI dependencies so `go mod tidy` keeps them in
// go.sum even though no production code imports them.
//
// Real pins (e.g. google.golang.org/protobuf/cmd/protoc-gen-go) land in T11
// when the proto codegen wiring is added. T02 ships the file with the build
// tag so the pattern exists ahead of time.
//
//go:build tools

package tools
