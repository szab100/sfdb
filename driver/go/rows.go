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

// Package sfdb: Implement rows functions
package sfdb

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

type rows struct {
	// client side cursor pointing to current record
	current_row int
	// rows as byte-encoded protobufs (byte stream)
	raw_rows []*any.Any
	// contains cached column names or column indexes
	column_names []string
	// contains cached column types
	column_types []*ColumnType
	// rows as text-encoded protobufs
	debugStrings []string
	message      *dynamic.Message
}

func NewRows(file_desc_set descriptor.FileDescriptorSet, row_msgs []*any.Any) (*rows, error) {
	if len(row_msgs) == 0 {
		return nil, errors.New("Empty resultset")
	}

	var importResolver desc.ImportResolver
	fileDescriptor, err := importResolver.CreateFileDescriptorFromSet(&file_desc_set)
	if err != nil {
		log.Error("Cannot read descriptor in proto response")
		return nil, err
	}

	msgTypeUrl := row_msgs[0].GetTypeUrl()
	msgTypeId := msgTypeUrl[strings.LastIndex(msgTypeUrl, "/")+1:]
	if len(msgTypeId) == 0 {
		return nil, errors.New("Invalid message id in proto response")
	}
	msgDescriptor := fileDescriptor.FindMessage(msgTypeId)
	if msgDescriptor == nil {
		log.Error("Cannot find descriptor for message in proto response.")
		return nil, errors.New("Cannot find descriptor for message in proto response.")
	}

	dmsg := dynamic.NewMessage(msgDescriptor)
	column_names := make([]string, len(dmsg.GetKnownFields()))
	column_types := make([]*ColumnType, len(dmsg.GetKnownFields()))
	for i, fd := range dmsg.GetKnownFields() {
		column_names[i] = fd.GetName()

		ct, err := NewColumnType(fd)
		if err != nil {
			return nil, err
		}
		column_types[i] = ct
	}

	return &rows{
		current_row:  0,
		raw_rows:     row_msgs,
		column_names: column_names,
		column_types: column_types,
		message:      dmsg,
	}, nil
}

func (r *rows) Close() error {
	log.V(debugLevel).Info("ROWS Close")
	return nil
}

func (r *rows) Next(values []driver.Value) error {
	log.V(debugLevel).Info("ROWS Next")
	if r.current_row >= len(r.raw_rows) {
		log.V(debugLevel).Info("ROWS Next - EOF")
		return io.EOF
	}

	err := r.message.Unmarshal(r.raw_rows[r.current_row].GetValue())
	if err != nil {
		return err
	}

	if len(values) != len(r.message.GetKnownFields()) {
		return errors.New("Internal error: num fields in rows differs from expected")
	}

	for i, fd := range r.message.GetKnownFields() {
		values[i] = r.message.GetField(fd)
		log.V(debugLevel).Info("ROWS Next - Value[", i, "]: '", values[i], "'")
	}

	r.current_row += 1

	return nil
}

func (r *rows) Columns() []string {
	log.V(debugLevel).Info("ROWS Columns")
	return r.column_names
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.column_types[index].DatabaseTypeName()
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	length, ok = r.column_types[index].Length()
	return
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	nullable, ok = r.column_types[index].Nullable()
	return
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	precision, scale, ok = r.column_types[index].DecimalSize()
	return
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.column_types[index].ScanType()
}
