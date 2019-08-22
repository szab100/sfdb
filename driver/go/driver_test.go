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
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	host       = "localhost"
	port       = "27910"
	dbName     = "testdb"
	params     = "loas=false"
	creds      = "admin:pass"
	tableIndex = ""

	connStr = fmt.Sprintf("%s@%s:%s/%s?%s",
		creds, host, port, dbName, params)

	db *sql.DB
)

func assertErr(t *testing.T, err error, msg string) {
	if err != nil {
		fmt.Println(err.Error())
		t.Fatalf("ERROR: ", msg)
	}
}

func setup() {
	// FIXME: table_index has to be unique per database
	tableIndex = fmt.Sprintf("Test_%d", time.Now().Unix())
}

func teardown() {
}

func initDB(t *testing.T, db *sql.DB) {
	// drop table first
	_, err := db.Exec("DROP TABLE Test;")

        // TODO: There is no IF EXISTS check for now
	// so we ignore only this type of error
        if err != nil && !strings.Contains(err.Error(), "No table named Test") {
                assertErr(t, err, "Should drop a Test table.")
        }

	_, err = db.Exec("CREATE TABLE Test (id int64, name string, age int64);")
	assertErr(t, err, "Should create a Test table.")
	_, err = db.Exec(
		fmt.Sprintf("CREATE INDEX %s ON Test (id);", tableIndex))
	assertErr(t, err, "Should create an index by id.")
}

/* Tests */

func TestMain(m *testing.M) {
	setup()
	exitcode := m.Run()
	teardown()
	os.Exit(exitcode)
}

func TestDriverRegister(t *testing.T) {
	db, err := sql.Open("sfdb", "anything")
	assertErr(t, err, "Should have a registered SFDB driver.")
	defer db.Close()
}

func TestDriverCloseConn(t *testing.T) {
	db, _ := sql.Open("sfdb", connStr)
	db.Close()
	_, err := db.Exec("anything")
	if err.Error() != "sql: database is closed" {
		t.Fatalf("Should return ErrConnDone error.")
	}
}

func TestDriverConnect(t *testing.T) {
	db, err := sql.Open("sfdb", connStr)
	defer db.Close()
	assertErr(t, err, "Should connect to SFDB.")
}

func TestDriverExecDropCreate(t *testing.T) {
	db, _ := sql.Open("sfdb", connStr)
	defer db.Close()
	initDB(t, db)
}

func TestDriverExecInsert(t *testing.T) {
	db, err := sql.Open("sfdb", connStr)
	defer db.Close()
	initDB(t, db)

	var signedId = 0
	var name = ""
	var names = strings.Split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "")
	var age = 0

	for i := 0; i < 10; i++ {
		signedId = i ^ 0xdeadbeef
		name = "ABC" + string(names[i])
		age = i
		query := fmt.Sprintf(
			"INSERT INTO Test (id, name, age) VALUES (%d, '%s', %d);",
				signedId, name, age)
		_, err = db.Exec(query)
		assertErr(t, err, "Should insert into Test table.")
	}

	db.QueryRow("SELECT id, name, age FROM Test;")
}
