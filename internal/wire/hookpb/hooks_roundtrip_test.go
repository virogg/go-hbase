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

// TestAllHookMessagesRoundTrip is the T42 Wave-5 "100% coverage" gate: every
// hookpb message must marshal/unmarshal cleanly at default-instance shape
// (proto.Marshal -> proto.Unmarshal -> equal). Fields added in later phases
// (T43+) flow through the same code path.
//
// Enumerates message types via the proto global registry, filters to
// virogg.hbasecop.v1, and asserts:
//
//   - default-instance bytes round-trip equal
//   - mutating one field (a synthetic byte tag) also round-trips, exercising
//     wire-tag handling beyond the no-op default.
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

			// Mutating round-trip: populate any byte/string field found via
			// reflection so the wire bytes are non-empty.
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
					// Skip enums, messages, doubles, floats; not needed for the
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
	// T41 hook surface + T42 Waves 1-4 give 80+ proto messages today (68 hook
	// Requests + shared HookContext/HookResponse + 12 helper types like
	// CellPair, FamilyPath, etc.). Fewer than 70 means something stopped
	// registering.
	if count < 70 {
		t.Fatalf("hookpb registry shows %d messages, want >=70 (T42 coverage gate)", count)
	}
}
