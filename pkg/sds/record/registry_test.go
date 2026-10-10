// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRegistryBasicAndIdempotency(t *testing.T) {
	reg := NewRegistry()

	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV1 := makeTestTypeDef("Order", fieldsV1, nil, nil, []int32{1}, nil)
	defV1.Id = 16

	// Register V1
	if err := reg.Register(defV1); err != nil {
		t.Fatalf("Register(defV1) error: %v", err)
	}

	// Lookup by ID
	lookupDef, md, ok := reg.LookupByID(16)
	if !ok || lookupDef == nil || md == nil {
		t.Fatalf("LookupByID(16) failed")
	}
	if lookupDef.GetName() != "testpkg.Order" {
		t.Errorf("lookupDef.Name = %q, want 'testpkg.Order'", lookupDef.GetName())
	}

	// Lookup by Name
	lookupDef2, md2, ok := reg.LookupByName("testpkg.Order")
	if !ok || lookupDef2 == nil || md2 == nil {
		t.Fatalf("LookupByName('testpkg.Order') failed")
	}

	// Idempotent re-registration of exact same definition
	if err := reg.Register(defV1); err != nil {
		t.Fatalf("idempotent Register(defV1) failed: %v", err)
	}

	// Compatible evolution V2
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV2 := makeTestTypeDef("Order", fieldsV2, nil, nil, []int32{1}, nil)
	defV2.Id = 16

	if err := reg.Register(defV2); err != nil {
		t.Fatalf("compatible evolution Register(defV2) error: %v", err)
	}

	// Verify registry updated to V2
	updatedDef, _, ok := reg.LookupByID(16)
	if !ok {
		t.Fatalf("LookupByID(16) after V2 failed")
	}
	if !bytes.Equal(updatedDef.GetFingerprint(), defV2.GetFingerprint()) {
		t.Errorf("updatedDef fingerprint did not update to V2")
	}

	// Incompatible change (removed field without reserving)
	fieldsV3Bad := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV3Bad := makeTestTypeDef("Order", fieldsV3Bad, nil, nil, []int32{1}, nil)
	defV3Bad.Id = 16

	if err := reg.Register(defV3Bad); err == nil {
		t.Fatalf("expected error on incompatible evolution, got nil")
	}
}

func TestRegistryExportImport(t *testing.T) {
	reg := NewRegistry()

	def1 := makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	def1.Id = 16

	def2 := makeTestTypeDef("Item", []*descriptorpb.FieldDescriptorProto{
		field("sku", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	def2.Id = 17

	if err := reg.Register(def1); err != nil {
		t.Fatalf("Register def1 error: %v", err)
	}
	if err := reg.Register(def2); err != nil {
		t.Fatalf("Register def2 error: %v", err)
	}

	exported := reg.Export()
	if len(exported.GetTypes()) != 2 {
		t.Fatalf("exported %d types, want 2", len(exported.GetTypes()))
	}

	// Import into new registry
	reg2 := NewRegistry()
	if err := reg2.Import(exported); err != nil {
		t.Fatalf("Import error: %v", err)
	}

	if _, _, ok := reg2.LookupByID(16); !ok {
		t.Errorf("reg2 missing ID 16")
	}
	if _, _, ok := reg2.LookupByID(17); !ok {
		t.Errorf("reg2 missing ID 17")
	}
}

func TestRegistryValidationErrors(t *testing.T) {
	reg := NewRegistry()

	// Type ID < 16
	defLow := &sdsv1.TypeDefinition{
		Id:          10,
		Name:        "test.Msg",
		Fingerprint: []byte("01234567890123456789012345678901"),
	}
	if err := reg.Register(defLow); !errors.Is(err, ErrTypeIDTooLow) {
		t.Errorf("Register ID 10 got error %v, want ErrTypeIDTooLow", err)
	}

	// Missing descriptors on new registration
	defNoDesc := &sdsv1.TypeDefinition{
		Id:          16,
		Name:        "test.Msg",
		Fingerprint: []byte("01234567890123456789012345678901"),
	}
	if err := reg.Register(defNoDesc); !errors.Is(err, ErrMissingDescriptors) {
		t.Errorf("Register without descriptors got error %v, want ErrMissingDescriptors", err)
	}

	// Fingerprint mismatch
	defMismatchedFP := makeTestTypeDef("Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	defMismatchedFP.Id = 16
	defMismatchedFP.Fingerprint = []byte("corrupted_fingerprint_0123456789")

	if err := reg.Register(defMismatchedFP); err == nil {
		t.Errorf("expected error on fingerprint mismatch, got nil")
	}

	// Repeated key field
	defRepeatedKey := makeTestTypeDef("OrderRepeatedKey", []*descriptorpb.FieldDescriptorProto{
		field("ids", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
	}, nil, nil, []int32{1}, nil)
	defRepeatedKey.Id = 18
	if err := reg.Register(defRepeatedKey); err == nil {
		t.Errorf("expected error on repeated key field, got nil")
	}

	// Message (non-scalar) key field
	defMsgKey := makeTestTypeDef("OrderMsgKey", []*descriptorpb.FieldDescriptorProto{
		field("sub", 1, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}, nil, nil, []int32{1}, nil)
	defMsgKey.Id = 19
	if err := reg.Register(defMsgKey); err == nil {
		t.Errorf("expected error on message-typed key field, got nil")
	}
}

func TestRegistryGoTypeResolution(t *testing.T) {
	reg := NewRegistry()

	// Register compiled Go type sdsv1.TxCommit
	commitMD := (&sdsv1.TxCommit{}).ProtoReflect().Descriptor()
	def, err := reg.RegisterDescriptor(commitMD, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor(TxCommit) error: %v", err)
	}

	msgType, err := reg.ResolveMessageType(def.GetId())
	if err != nil {
		t.Fatalf("ResolveMessageType error: %v", err)
	}

	// Verify that instantiated message is the concrete Go type
	instance := msgType.New().Interface()
	if _, ok := instance.(*sdsv1.TxCommit); !ok {
		t.Errorf("instance type = %T, want *sdsv1.TxCommit", instance)
	}
}

func TestRegistryConcurrentUse(t *testing.T) {
	reg := NewRegistry()

	// Pre-populate one type so lookups immediately find something
	initialDef, err := reg.RegisterMessage(&sdsv1.TxCommit{}, 1)
	if err != nil {
		t.Fatalf("RegisterMessage error: %v", err)
	}

	var wg sync.WaitGroup
	const (
		numGoroutines = 8
		iterations    = 100
	)

	// Goroutines registering messages concurrently
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				switch (gid + i) % 4 {
				case 0:
					_, _ = reg.RegisterMessage(&sdsv1.SnapshotPointer{}, 1)
				case 1:
					_, _ = reg.RegisterMessage(&sdsv1.TxCommit{}, 1)
				case 2:
					_, _ = reg.RegisterDescriptor((&sdsv1.OpRecord{}).ProtoReflect().Descriptor(), 1)
				case 3:
					def := makeTestTypeDef("DynamicMsg", []*descriptorpb.FieldDescriptorProto{
						field("f1", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
					}, nil, nil, []int32{1}, nil)
					def.Id = uint32(100 + (gid % 5))
					_ = reg.Register(def)
				}
			}
		}(g)
	}

	// Goroutines reading concurrently: LookupByID, LookupByName, ResolveMessageType
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _, _ = reg.LookupByID(initialDef.GetId())
				_, _, _ = reg.LookupByName(initialDef.GetName())
				_, _ = reg.ResolveMessageType(initialDef.GetId())
			}
		}(g)
	}

	// Goroutines exporting and importing concurrently
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				exp := reg.Export()
				if exp != nil && len(exp.GetTypes()) > 0 {
					other := NewRegistry()
					_ = other.Import(exp)
				}
			}
		}()
	}

	wg.Wait()
}

func TestRegistryConsistentExportUnderConcurrentWrites(t *testing.T) {
	reg := NewRegistry()

	const numTypes = 50
	typeDefs := make([]*sdsv1.TypeDefinition, numTypes)
	for i := 0; i < numTypes; i++ {
		typeName := fmt.Sprintf("SnapType%d", i)
		fields := []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}
		def := makeTestTypeDef(typeName, fields, nil, nil, []int32{1}, nil)
		def.Id = MinAppTypeID + uint32(i)
		typeDefs[i] = def
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Reader goroutines continuously check Export consistency (no torn maps)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					exp := reg.Export()
					types := exp.GetTypes()
					n := len(types)
					// Verify exported types represent a valid prefix of registered versions
					for i, def := range types {
						expectedID := MinAppTypeID + uint32(i)
						if def.GetId() != expectedID {
							t.Errorf("torn export detected: index %d has ID %d, expected consecutive %d", i, def.GetId(), expectedID)
						}
						expectedName := fmt.Sprintf("testpkg.SnapType%d", i)
						if def.GetName() != expectedName {
							t.Errorf("torn export detected: index %d has name %q, expected %q", i, def.GetName(), expectedName)
						}
					}
					// Also verify that LookupByID and LookupByName match for all entries in the snapshot
					if n > 0 {
						last := types[n-1]
						if ldef, _, ok := reg.LookupByID(last.GetId()); !ok || ldef == nil {
							t.Errorf("LookupByID(%d) failed for exported type", last.GetId())
						}
						if ldef, _, ok := reg.LookupByName(last.GetName()); !ok || ldef == nil {
							t.Errorf("LookupByName(%q) failed for exported type", last.GetName())
						}
					}
				}
			}
		}()
	}

	// Writer goroutine sequentially registers new types
	for _, def := range typeDefs {
		if err := reg.Register(def); err != nil {
			t.Fatalf("Register error: %v", err)
		}
	}

	close(stop)
	wg.Wait()

	finalExp := reg.Export()
	if len(finalExp.GetTypes()) != numTypes {
		t.Fatalf("final export count = %d, want %d", len(finalExp.GetTypes()), numTypes)
	}
}

func TestRegistryRejectsUnresolvableDefinition(t *testing.T) {
	reg := NewRegistry()

	// 1. Definition where message name is not found in descriptors
	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	def := makeTestTypeDef("ActualMsg", fields, nil, nil, []int32{1}, nil)
	def.Id = 16
	// Mismatch the definition name so resolveMessageDescriptor fails
	def.Name = "testpkg.NonExistentMsg"

	err := reg.Register(def)
	if err == nil {
		t.Fatalf("expected error registering unresolvable definition, got nil")
	}

	// Verify that unresolvable type was not registered
	if _, _, ok := reg.LookupByName(def.GetName()); ok {
		t.Errorf("LookupByName succeeded for unresolvable definition")
	}
	if _, _, ok := reg.LookupByID(def.GetId()); ok {
		t.Errorf("LookupByID succeeded for unresolvable definition")
	}
	if _, err := reg.ResolveMessageType(def.GetId()); !errors.Is(err, ErrTypeNotRegistered) {
		t.Errorf("ResolveMessageType error = %v, want ErrTypeNotRegistered", err)
	}
}
