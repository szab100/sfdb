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
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "sfdb/flags.h"
#include "sfdb/modules.h"
#include "sfdb/service_impl.h"

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

  ::sfdb::Modules modules;
  modules.Init();
  auto server_builder = modules.server_builder();

  ::sfdb::SfdbServiceImpl sfdb_service(&modules);
  server_builder->RegisterService(&sfdb_service);

  // start server
  auto server = server_builder->BuildAndStart();
  LOG(INFO) << "SFDB server is ready on port " << GetFlag(FLAGS_port);
  server->Wait();

  return 0;
}
