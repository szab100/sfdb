/*
 * Copyright (c) 2019 Google LLC.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package sfdb

import (
	"errors"
	"reflect"

	log "github.com/golang/glog"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
)

type ColumnType struct {
	column_name     string
	proto_type_name string
	reflect_type    reflect.Type
}

func NewColumnType(field *desc.FieldDescriptor) (*ColumnType, error) {

	var rType reflect.Type

	dpType := field.GetType()

	switch dpType {
	case dpb.FieldDescriptorProto_TYPE_FIXED32,
		dpb.FieldDescriptorProto_TYPE_UINT32:
		rType = reflect.TypeOf(uint32(0))
	case dpb.FieldDescriptorProto_TYPE_SFIXED32,
		dpb.FieldDescriptorProto_TYPE_INT32,
		dpb.FieldDescriptorProto_TYPE_SINT32:
		rType = reflect.TypeOf(int32(0))
	case dpb.FieldDescriptorProto_TYPE_FIXED64,
		dpb.FieldDescriptorProto_TYPE_UINT64:
		rType = reflect.TypeOf(uint64(0))
	case dpb.FieldDescriptorProto_TYPE_SFIXED64,
		dpb.FieldDescriptorProto_TYPE_INT64,
		dpb.FieldDescriptorProto_TYPE_SINT64:
		rType = reflect.TypeOf(int64(0))
	case dpb.FieldDescriptorProto_TYPE_FLOAT:
		rType = reflect.TypeOf(float32(0.0))
	case dpb.FieldDescriptorProto_TYPE_DOUBLE:
		rType = reflect.TypeOf(float64(0.0))
	case dpb.FieldDescriptorProto_TYPE_BOOL:
		rType = reflect.TypeOf(false)
	case dpb.FieldDescriptorProto_TYPE_BYTES:
		rType = reflect.TypeOf([]byte(nil))
	case dpb.FieldDescriptorProto_TYPE_STRING:
		rType = reflect.TypeOf("")
	case dpb.FieldDescriptorProto_TYPE_ENUM:
	default:
		// unsupported type
		log.Error("Unsupported data type in proto descriptor: ", rType.Name())
		return nil, errors.New("Unsupported data type in proto descriptor: " + rType.Name())
	}

	return &ColumnType{
		column_name:     field.GetName(),
		proto_type_name: dpType.String(),
		reflect_type:    rType,
	}, nil
}

// TODO: implement conversion of prototypes to generic DB types
func (ci *ColumnType) DatabaseTypeName() string {
	return ci.proto_type_name
}

// TODO: implement
func (ci *ColumnType) DecimalSize() (precision, scale int64, ok bool) {
	ok = false
	return
}

// TODO: implement
func (ci *ColumnType) Length() (length int64, ok bool) {
	ok = false
	return
}

func (ci *ColumnType) Name() string {
	return ci.column_name
}

// TODO: implement
func (ci *ColumnType) Nullable() (nullable, ok bool) {
	ok = false
	return
}

// TODO: implement
func (ci *ColumnType) ScanType() reflect.Type {
	return ci.reflect_type
}
