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
#include "raft/raft.h"

#include "raft/service_impl.h"

namespace raft {

using ::absl::string_view;
using ::util::Status;

Member::Member(const Options &opts) : impl_(opts) {}

void Member::Start() { impl_.Start(); }

void Member::Stop() { impl_.Stop(); }

void Member::Append(string_view msg) { impl_.Append(msg); }

Status Member::Write(string_view msg, void *arg) {
  return impl_.Write(msg, arg);
}

}  // namespace raft
