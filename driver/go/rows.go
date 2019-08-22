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
	log "github.com/golang/glog"
)

type rows struct {
	// rows as byte-encoded protobufs (byte stream), big-endian byte order.
	// Example: query returns 1 item: [[8 1 18 8 65 65 65 65 65 65 65 65]]
	// 08 - wiretype = 0, varint:
	//	0000 1000
	//		- drop MSB
	//	 000 1000
	//		- last 3 bits represent a wiretype (0b000 -> 0x0)
	//		- 4th least significant bit - proto field number = 1
	// 01 - value = 1
	//	0000 0001
	//		- no MSB, value is 0x1
	// 18 - wiretype = 2, string:
	//	0001 0010
	//		- wiretype = 010 -> 0x2, string
	//		- field number = 0
	// 08 - string length
	// 65 .. 65 - 0x41
	raw [][]byte
	// parsed rows pointers to Row structs
	parsed []*Row
	// rows as text-encoded protobufs
	debugStrings []string
}

// Row is a struct of deserialized protobuf from "rows->raw" field.
type Row struct {
	columns []string
	values  []interface{}
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next([]driver.Value) error {
	return nil
}

func (r *rows) Columns() []string {
	return nil
}

// Parse (deserialize) rows->raw protobufs
func (r *rows) Parse() error {
	log.Info("BYTE STREAM: ", r.raw)
	return nil
}
