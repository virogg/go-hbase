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
	if count < 70 {
		t.Fatalf("hookpb registry shows %d messages, want >=70 (T42 coverage gate)", count)
	}
}
