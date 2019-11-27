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
#include "sfdb/flags.h"

#include <string>

// -----------------------------------------------------------------------------
// Required configuration
// -----------------------------------------------------------------------------

ABSL_FLAG(int32, port, 27910, "Main HTTP port");

// -----------------------------------------------------------------------------
// RAFT mode
// -----------------------------------------------------------------------------
ABSL_FLAG(string, raft_impl, "raft",
          "A raft implementation. Possible value:"
          "\traft - built in raft implementation. GRPC will be used as RPC implementation"
          "\tbraft - BRAFT implementation. BRPC will be used as RPC implementation");

ABSL_FLAG(string, raft_my_target, "127.0.0.1:27910",
          "A SmartStub-compatible Stubby target of this binary.");

ABSL_FLAG(string, raft_targets, "127.0.0.1:27910:0",
          "The list of all (or other) RAFT targets in the cluster.");

// -----------------------------------------------------------------------------
// Logging related flags
// -----------------------------------------------------------------------------

ABSL_FLAG(int32, log_v, 0, "Alias for glog --v flag");
ABSL_FLAG(bool, log_alsologtostderr, false, "Alias for glog --alsologtostderr flag");
