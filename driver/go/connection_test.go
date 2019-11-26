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
	"fmt"
	"reflect"
	"testing"
)

func assertEqual(t *testing.T, want, have interface{}) {
	if want != have {
		t.Fatalf(fmt.Sprintf("want:%v, have:%v", want, have))
	}
}

func TestParseConnString(t *testing.T) {
	conn := newConnection()

	// full conn string
	connString := "admin:pass@localhost:27910/testdb?ttl=5"

	err := ParseConnString(conn, connString)
	if err != nil {
		t.Fatalf("ERROR: ", err.Error(), "")
	}
	expected := &Connection{
		creds:  "admin:pass",
		addr:   "localhost:27910",
		dbName: "testdb",
		params: &DBParams{ttl: 5},
	}
	if !reflect.DeepEqual(conn, expected) {
		t.Fatalf(fmt.Sprintf("want:%v, have:%v", expected, conn))
	}

	// empty conn string
	err = ParseConnString(conn, "   ")
	assertEqual(t, err.Error(), "empty Connection string")

	// conn string without DB params, should have a default TTL
	conn = newConnection()
	connString = "admin:pass@localhost:27910/testdb"
	err = ParseConnString(conn, connString)
	assertEqual(t, conn.params.ttl, defaultTTLSeconds)
	assertEqual(t, conn.params.includeDebugStrings, false)
}

func TestParseConnStringAddressPortOnly(t *testing.T) {
	conn := newConnection()

	connString := "localhost:27910"
	err := ParseConnString(conn, connString)
	assertEqual(t, nil, err)
	assertEqual(t, "localhost:27910", conn.addr)
}

func TestParseConnStringWrongCreds(t *testing.T) {
	conn := newConnection()

	connString := "admin@localhost:27910/testdb"
	err := ParseConnString(conn, connString)
	assertEqual(t, "admin", conn.creds)

	connString = "admin@admin@localhost:27910/testdb"
	err = ParseConnString(conn, connString)
	assertEqual(t, err.Error(), "too many @ symbols")
}

func TestParseConnStringWrongAddr(t *testing.T) {
	conn := newConnection()

	connString := "admin@localhost:27910/testdb/foo"
	err := ParseConnString(conn, connString)
	assertEqual(t, err.Error(), "too many / symbols")
}

func TestParseConnStringDBParams(t *testing.T) {
	conn := newConnection()

	connString := "localhost:27910/testdb?includeDebugStrings=1&ttl=3"
	err := ParseConnString(conn, connString)
	assertEqual(t, err, nil)
	assertEqual(t, conn.params.includeDebugStrings, true)
	assertEqual(t, conn.params.ttl, 3)
}

func TestParseConnStringWrongDBParams(t *testing.T) {
	conn := newConnection()

	// broken conn string
	connString := "admin@localhost:27910/testdb?foo=false?baz=1"
	err := ParseConnString(conn, connString)
	assertEqual(t, err.Error(), "too many ? symbols")

	// irrelevant db param keys (foo)
	connString = "localhost:27910/testdb?foo=false&ttl=1337"
	err = ParseConnString(conn, connString)
	assertEqual(t, err, nil)
	assertEqual(t, conn.params.ttl, 1337)

	// irrelevant db param vals
	connString = "localhost:27910/testdb?includeDebugStrings=123"
	err = ParseConnString(conn, connString)
	assertEqual(t, err.Error(), "strconv.ParseBool: parsing \"123\": invalid syntax")
}
