// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package hbasecop

import (
	"context"
	"reflect"
	"testing"
)

func TestUnimplementedObserversAreNoOps(t *testing.T) {
	surfaces := []struct {
		name string
		impl any
		want int // expected method count, guards against an empty reflect walk
	}{
		{"RegionObserver", UnimplementedRegionObserver{}, 68},
		{"MasterObserver", UnimplementedMasterObserver{}, 20},
		{"RegionServerObserver", UnimplementedRegionServerObserver{}, 9},
		{"WALObserver", UnimplementedWALObserver{}, 4},
		{"BulkLoadObserver", UnimplementedBulkLoadObserver{}, 2},
	}

	ctxType := reflect.TypeFor[context.Context]()

	for _, s := range surfaces {
		t.Run(s.name, func(t *testing.T) {
			v := reflect.ValueOf(s.impl)
			vt := v.Type()
			if vt.NumMethod() != s.want {
				t.Fatalf("%s exposes %d methods, expected %d (update the test when the surface changes)",
					s.name, vt.NumMethod(), s.want)
			}
			for i := range vt.NumMethod() {
				method := vt.Method(i)
				fn := v.Method(i)
				ft := fn.Type()

				args := make([]reflect.Value, ft.NumIn())
				for j := range ft.NumIn() {
					in := ft.In(j)
					switch {
					case in.Implements(ctxType):
						args[j] = reflect.ValueOf(t.Context())
					case in.Kind() == reflect.Pointer:
						// Non-nil pointer to a zero-value request; no-ops ignore it.
						args[j] = reflect.New(in.Elem())
					default:
						// ObserverEnv and any other concrete value type.
						args[j] = reflect.Zero(in)
					}
				}

				out := fn.Call(args)

				errVal := out[len(out)-1]
				if !errVal.IsNil() {
					t.Errorf("%s.%s returned non-nil error: %v", s.name, method.Name, errVal.Interface())
				}
				if len(out) == 2 {
					hr, ok := out[0].Interface().(HookResult)
					if !ok {
						t.Errorf("%s.%s first return is not HookResult", s.name, method.Name)
						continue
					}
					if hr.Bypass || len(hr.BlockedIndices) != 0 || len(hr.ResultCells) != 0 {
						t.Errorf("%s.%s returned a non-inert HookResult: %+v", s.name, method.Name, hr)
					}
				}
			}
		})
	}
}
