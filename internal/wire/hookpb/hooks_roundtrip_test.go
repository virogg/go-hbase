// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hookpb_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/virogg/go-hbase/internal/wire/hookpb"
)

// TestAllHookMessagesRoundTrip is the T42 Wave-5 "100% coverage" gate:
// every message in the hookpb package must marshal/unmarshal cleanly at
// its default-instance shape (proto.Marshal -> proto.Unmarshal -> equal).
// New per-hook fields appended in later phases (T43+) keep flowing
// through this check because they share the same code path.
//
// The check enumerates message types via the proto global registry,
// filters to the virogg.hbasecop.v1 package and asserts:
//
//   - default-instance bytes round-trip equal
//   - explicitly mutating one field (a synthetic byte tag) round-trips
//     too — exercises wire-tag handling beyond the no-op default.
func TestAllHookMessagesRoundTrip(t *testing.T) {
	count := 0
	protoregistry.GlobalTypes.RangeMessages(func(mt protoreflect.MessageType) bool {
		name := string(mt.Descriptor().FullName())
		if !strings.HasPrefix(name, "virogg.hbasecop.v1.") {
			return true
		}
		count++
		t.Run(name, func(t *testing.T) {
			msg := mt.New().Interface()

			// Default-instance round-trip.
			b, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("marshal default: %v", err)
			}
			roundtrip := mt.New().Interface()
			if err := proto.Unmarshal(b, roundtrip); err != nil {
				t.Fatalf("unmarshal default: %v (bytes=%d)", err, len(b))
			}
			if !proto.Equal(msg, roundtrip) {
				t.Fatalf("default round-trip not equal")
			}

			// Mutating round-trip — populate any byte/string field we can
			// find via reflection so the wire bytes are non-empty.
			mutated := mt.New()
			fields := mt.Descriptor().Fields()
			for i := 0; i < fields.Len(); i++ {
				fd := fields.Get(i)
				if fd.IsList() || fd.IsMap() {
					continue
				}
				switch fd.Kind() {
				case protoreflect.BytesKind:
					mutated.Set(fd, protoreflect.ValueOfBytes([]byte("rt")))
				case protoreflect.StringKind:
					mutated.Set(fd, protoreflect.ValueOfString("rt"))
				case protoreflect.BoolKind:
					mutated.Set(fd, protoreflect.ValueOfBool(true))
				case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
					mutated.Set(fd, protoreflect.ValueOfInt32(7))
				case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
					mutated.Set(fd, protoreflect.ValueOfUint32(7))
				case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
					mutated.Set(fd, protoreflect.ValueOfInt64(7))
				case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
					mutated.Set(fd, protoreflect.ValueOfUint64(7))
				default:
					// Skip enums, messages, doubles, floats — not needed for the
					// smoke check.
				}
			}
			b, err = proto.Marshal(mutated.Interface())
			if err != nil {
				t.Fatalf("marshal mutated: %v", err)
			}
			parsed := mt.New().Interface()
			if err := proto.Unmarshal(b, parsed); err != nil {
				t.Fatalf("unmarshal mutated: %v", err)
			}
			if !proto.Equal(mutated.Interface(), parsed) {
				t.Fatalf("mutated round-trip not equal")
			}
		})
		return true
	})
	// The T41 hook surface + T42 Waves 1-4 give us 80+ proto messages
	// today (68 hook Requests + shared HookContext/HookResponse + 12
	// helper types like CellPair, FamilyPath, …). If the registry walk
	// finds fewer than 70 messages, something stopped registering.
	if count < 70 {
		t.Fatalf("hookpb registry shows %d messages, want >=70 (T42 coverage gate)", count)
	}
}
