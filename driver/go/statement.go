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
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

type stmt struct {
	conn  *Connection
	query string
}

func (s *stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *stmt) NumInput() int {
	return strings.Count(s.query, "?")
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (s *stmt) CheckNamedValue(val *driver.NamedValue) error {
	return nil
}

func (s *stmt) QueryRow(args ...interface{}) *driver.Rows {
	return nil
}

func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) *driver.Rows {
	return nil
}

// Query is DEPRECATED.
// Calls QueryContext to implement StmtQueryContext interface.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i,
			Value:   arg,
		}
	}
	return s.QueryContext(context.Background(), namedArgs)
}

// QueryContext executes a prepared query statement with the given arguments
// and returns the query results as a *Rows.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	processedQuery, err := interpolate(s.query, args)
	if err != nil {
		return nil, err
	}
	return queryRPC(ctx, processedQuery, s.conn)

}

// Exec is DEPRECATED.
// Calls ExecContext to implement StmtExecContext interface.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Name:    "",
			Ordinal: i,
			Value:   arg,
		}
	}
	return s.ExecContext(context.Background(), namedArgs)
}

// ExecContext must honor the context timeout and return when it is canceled.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	processedQuery, err := interpolate(s.query, args)
	if err != nil {
		return nil, err
	}
	return execRPC(ctx, processedQuery, s.conn)
}

// TODO: Add sanitization here
// for SQLi like 'OR 1=1', 'AND 1=2' etc.
func sanitizeQuery(query string) error {
	if len(query) == 0 {
		return fmt.Errorf("query is empty")
	}
	return nil
}

// Finds a common ? placeholders and replace with the custom @p1, @p2, ..
func interpolate(query string, args []driver.NamedValue) (string, error) {
	if err := sanitizeQuery(query); err != nil {
		return "", err
	}

	tmp := query

	for i := 1; i < len(query)+1; i++ {
		placeholder := fmt.Sprintf("@p%d", i)
		tmp = strings.Replace(tmp, "?", placeholder, 1)
	}

	var replacements []string
	for _, arg := range args {
		var placeholder string
		if arg.Name != "" {
			placeholder = fmt.Sprintf("@%s", arg.Name)
		} else {
			placeholder = fmt.Sprintf("@p%d", arg.Ordinal)
		}
		val := fmt.Sprintf("%v", arg.Value)
		replacements = append(replacements, placeholder, val)
	}

	r := strings.NewReplacer(replacements...)
	query = r.Replace(tmp)

	// make sure we have a semicolon at the end of query
	if query[len(query)-1:] != ";" {
		query += ";"
	}

	return query, nil
}

// queryRPC sends a preprocessed query in statement to SFDB via RPC,
// then converts the Protobuf response to rows.
func queryRPC(ctx context.Context, query string, conn *Connection) (driver.Rows, error) {
	pbResp, err := SendRPC(ctx, query, conn)
	if err != nil {
		return nil, err
	}

	rows, err := NewRows(*pbResp.Descriptors, pbResp.Rows)
	return rows, err
}

// execRPC sends a preprocessed query in statement to SFDB via RPC.
func execRPC(ctx context.Context, query string, conn *Connection) (driver.Result, error) {
	pbResp, err := SendRPC(ctx, query, conn)
	if err != nil {
		return nil, err
	}

	result, err := NewResult(pbResp)
	return result, err
}
