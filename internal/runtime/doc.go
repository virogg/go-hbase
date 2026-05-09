// Package runtime is the Go-side event loop: it owns the shmem channel,
// dispatches inbound requests to user Observer handlers and emits heartbeats.
package runtime
