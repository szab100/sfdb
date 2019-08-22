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
	"fmt"
	"time"

	log "github.com/golang/glog"
	sfdb_pb "github.com/googlegsa/sfdb/api_go_proto"
)

// SendRPC constructs the Protobuf request
// and execute the RPC ExecSql in SFDB target.
func SendRPC(ctx context.Context, query string, conn *Connection) (*sfdb_pb.ExecSqlResponse, error) {
	if conn.params == nil {
		log.Fatalln("Null pointer to DBParams")
	}
	params := *conn.params

	var ttlSeconds = params.ttl

	// create a context with TTL
	ctx, cancel := context.WithTimeout(ctx, time.Duration(ttlSeconds)*time.Second)
	defer func() {
		cancel()
	}()

	// create unbuffered channels and use them in goroutine
	respCh := make(chan *sfdb_pb.ExecSqlResponse)
	errCh := make(chan error)
	go func() {
		protoreq := sfdb_pb.ExecSqlRequest{
			Sql:                 &query,
			IncludeDebugStrings: &conn.params.includeDebugStrings,
		}

		protoresp, err := (*conn.stub).ExecSql(context.TODO(), &protoreq)
		if err != nil {
			errCh <- err
			return
		}
		respCh <- protoresp
	}()

	select {
	// Context is cancelled due to TTL
	case <-ctx.Done():
		err := fmt.Errorf(fmt.Sprintf("RPC timeout: %d sec", ttlSeconds))
		return nil, err
	// Protobuf success response
	case ret := <-respCh:
		return ret, nil
	// Error occurred
	case err := <-errCh:
		return nil, err
	}
}

