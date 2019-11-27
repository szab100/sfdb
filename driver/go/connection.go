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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	sfdb_pb "github.com/googlegsa/sfdb/grpc_sfdb_service_go_proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	defaultTTLSeconds = 5
)

// Connection struct of SFDB driver RPC conn.
type Connection struct {
	rpcConn *grpc.ClientConn
	// username:password
	creds string
	// ipv4:port, ipv6:port, hostname:port
	addr   string
	dbName string
	// parsed to DBParams struct: param1=value1&...&paramN=valueN
	params *DBParams
	// RPC stub object pointer
	stub *sfdb_pb.SfdbServiceClient
}

// DBParams contains structured fields of db params
type DBParams struct {
	// TTL for RPC request.
	// This should be <= `deadline` protobuf field if it's set.
	ttl int
	// If true, then SFDB returns protobuf response as
	// stringified JSON format (on SELECT queries),
	// if false, then SFDB returns protobuf with the serialized, compressed
	// protobufs, and SFDB driver will de-serialize into Row structs.
	includeDebugStrings bool
}

// Parse dbParams part of connection string
func (p *DBParams) Parse(dbParams string) error {
	params := strings.Split(dbParams, "&")

	for i := 0; i < len(params); i++ {
		pair := strings.Split(params[i], "=")
		if len(pair) != 2 {
			continue
		}
		k, v := pair[0], pair[1]

		switch k {
		case "ttl":
			ttl, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
			// convert int64 to int
			p.ttl = int(ttl)
		case "includeDebugStrings":
			flag, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			p.includeDebugStrings = flag
		}
	}

	return nil
}

// Begin function starts a transaction.
func (c *Connection) Begin() (driver.Tx, error) {
	err := errors.New("sfdb does not support transactions")
	return nil, err
}

// Prepare returns a prepared statement, bound to this Connection.
func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := sanitizeQuery(query); err != nil {
		return nil, err
	}
	s := &stmt{
		conn:  c,
		query: query,
	}
	return s, nil
}

// Close RPC connection with SFDB target.
func (c *Connection) Close() error {
	if c.rpcConn != nil {
		fmt.Println("Closing RPC connection..")
		c.rpcConn.Close()
		c.rpcConn = nil
	}
	return nil
}

func (c *Connection) QueryContext(ctx context.Context, query string,
	args []driver.NamedValue) (driver.Rows, error) {
	processedQuery, err := interpolate(query, args)
	if err != nil {
		return nil, err
	}
	return QueryRPC(ctx, processedQuery, c)
}

func (c *Connection) ExecContext(ctx context.Context, query string,
	args []driver.NamedValue) (driver.Result, error) {
	processedQuery, err := interpolate(query, args)
	if err != nil {
		return nil, err
	}
	return ExecRPC(ctx, processedQuery, c)
}

// ParseConnString parses connection string to Connection struct.
// Example of Connection string:
// [username[:password]@][address]/dbname[?param1=value1&...&paramN=valueN]
func ParseConnString(c *Connection, connString string) error {
	tmp := strings.TrimSpace(connString)

	if len(tmp) == 0 {
		err := errors.New("empty Connection string")
		return err
	}

	// username:password
	creds := strings.Split(tmp, "@")
	if len(creds) > 2 {
		err := errors.New("too many @ symbols")
		return err
	} else if len(creds) == 2 {
		c.creds = creds[0]
		tmp = creds[1]
	}

	// addr:port
	addrs := strings.Split(tmp, "/")
	if len(addrs) > 2 {
		err := errors.New("too many / symbols")
		return err
	} else {
		c.addr = addrs[0]
		if c.addr == "" {
			err := errors.New("invalid address")
			return err
		}
		if len(addrs) == 2 {
			tmp = addrs[1]
		} else {
			tmp = ""
		}
	}

	// dbname
	dbname := strings.Split(tmp, "?")
	if len(dbname) > 2 {
		err := errors.New("too many ? symbols")
		return err
	} else if len(dbname) == 2 {
		c.dbName = dbname[0]
		// db params
		err := c.params.Parse(dbname[1])
		if err != nil {
			return err
		}
	} else {
		c.dbName = tmp
	}

	return nil
}

func newConnection() *Connection {
	return &Connection{
		params: &DBParams{ttl: defaultTTLSeconds},
	}
}

// Establishes actual connection to GRPC server
func ConnectToService(address string, timeout time.Duration) (*grpc.ClientConn, *sfdb_pb.SfdbServiceClient, error) {
	rpcConn, err := grpc.Dial(address, grpc.WithBlock(), grpc.WithTimeout(timeout), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}

	stub := sfdb_pb.NewSfdbServiceClient(rpcConn)
	return rpcConn, &stub, nil
}

// Connect establishes RPC Connection with SFDB target host
// with the given Connection string.
// TODO: Use a pool of connections?
func Connect(connString string) (*Connection, error) {
	var err error
	conn := newConnection()

	// 1. parse Connection string
	err = ParseConnString(conn, connString)
	if err != nil {
		return nil, err
	}

	// 2. establish RPC Connection
	conn.rpcConn, conn.stub, err = ConnectToService(conn.addr, 1*time.Second)

	return conn, err
}

// Ping implements driver.Pinger interface
func (c *Connection) Ping(ctx context.Context) (err error) {
	if c.rpcConn == nil {
		return driver.ErrBadConn
	}

	if c.rpcConn.GetState() != connectivity.Ready && c.rpcConn.GetState() != connectivity.Idle {
		return driver.ErrBadConn
	}

	return nil
}

func (c *Connection) Redirect(new_host string) (err error) {
	parts := strings.Split(new_host, ":")
	if len(parts) < 2 {
		return errors.New(fmt.Sprintf("Invalid redirect address: %s", new_host))
	}

	var new_service_address = parts[0] + ":" + parts[1]

	rpcConn, stub, err := ConnectToService(new_service_address, 1*time.Second)

	if err != nil {
		return err
	}

	c.rpcConn = rpcConn
	c.stub = stub

	return nil
}
