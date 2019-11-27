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
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "sfdb/flags.h"

#include "server/grpc_sfdb_server.h"
#include "server/brpc_sfdb_server.h"

using ::absl::GetFlag;
using ::absl::Seconds;
using ::absl::StrCat;
using ::absl::Substitute;

int main(int argc, char **argv) {
  google::InitGoogleLogging(*argv);
  absl::ParseCommandLine(argc, argv);


  // Setup some logging related flags.
  // NOTE: glog uses gflags which are defined in global scope
  //       so we have to set them like this using aliases
  FLAGS_v = GetFlag(FLAGS_log_v);
  FLAGS_alsologtostderr = GetFlag(FLAGS_log_alsologtostderr);

  auto raft_impl = GetFlag(FLAGS_raft_impl);
  std::unique_ptr<sfdb::SfdbServer> server;
  if (raft_impl == "raft") {
    server = absl::make_unique<sfdb::GrpcSfdbServer>();
  } else if (raft_impl == "braft") {
    server = absl::make_unique<sfdb::BrpcSfdbServer>();
  } else {
    LOG(FATAL) << "Unknown RAFT impementation: " << raft_impl;
  }

  std::vector<std::string> address_parts = absl::StrSplit(GetFlag(FLAGS_raft_my_target), ":");
  CHECK(address_parts.size() == 2);
  int port;
  CHECK(absl::SimpleAtoi(address_parts[1], &port));

  if (server->StartAndWait(address_parts[0], port, GetFlag(FLAGS_raft_targets))) {
    LOG(FATAL) << "Failed to launch server";
  }

  return 0;
}
