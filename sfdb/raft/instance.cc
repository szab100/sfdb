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
#include "sfdb/raft/instance.h"

#include <memory>

#include "absl/memory/memory.h"
#include "absl/strings/str_split.h"
#include "glog/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/message.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/typed_ast.h"
#include "sfdb/engine/engine.h"
#include "sfdb/flags.h"
#include "sfdb/raft/mutation.pb.h"
#include "sfdb/sql/parser.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::GetFlag;
using ::absl::make_unique;
using ::absl::SkipEmpty;
using ::absl::string_view;
using ::absl::StrSplit;
using ::google::protobuf::Message;
using ::util::Clock;
using ::util::InternalError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

RaftInstance::RaftInstance(Db *db, grpc::ServerBuilder *server_builder,
                           ::util::Clock *clock)
    : server_builder_(server_builder), db_(db), clock_(clock) {
  ::raft::Options options;
  options.my_target = GetFlag(FLAGS_raft_my_target);
  options.targets = StrSplit(GetFlag(FLAGS_raft_targets), ",", SkipEmpty());
  options.server_builder = server_builder;
  options.on_append = [this](string_view msg, void *arg) {
    return OnAppend(msg, arg);
  };
  options.clock = clock_;
  raft_ = make_unique<::raft::Member>(options);
  raft_->Start();
}

RaftInstance::~RaftInstance() { raft_->Stop(); }

Status RaftInstance::ExecSql(const ExecSqlRequest &request,
                             ExecSqlResponse *response) {
  // TODO: treat reads and writes differently.
  // StatusOr<std::unique_ptr<Ast>> ast_so = Parse(request.sql());
  // if (!ast_so.ok()) return ast_so.status();
  // std::unique_ptr<Ast> ast = std::move(ast_so.ValueOrDie());

  Mutation mut;
  mut.set_time_nanos(ToUnixNanos(clock_->TimeNow()));
  mut.set_sql(request.sql());
  std::pair<const ExecSqlRequest *, ExecSqlResponse *> p{&request, response};
  return raft_->Write(mut.SerializeAsString(), (void *)&p);
}

Status RaftInstance::OnAppend(string_view msg, void *arg) {
  Mutation mut;
  if (!mut.ParseFromString(string(msg))) {
    LOG(ERROR) << "Massive fail parsing Mutation proto!";
    return InternalError("oopsie");
  }

  VLOG(2) << "Executing SQL statement @" << mut.time_nanos();
  StatusOr<std::unique_ptr<Ast>> ast_so = Parse(mut.sql());
  if (!ast_so.ok()) return ast_so.status();
  std::unique_ptr<Ast> ast = std::move(ast_so.ValueOrDie());
  std::unique_ptr<ProtoPool> tmp_pool = db_->pool->Branch();
  if (ast->IsMutation()) {
    return ExecuteWrite(std::move(ast), tmp_pool.get(), db_);
  } else {
    std::vector<std::unique_ptr<Message>> rows;
    Status s = ExecuteRead(std::move(ast), tmp_pool.get(), db_, &rows);
    if (!s.ok() || !arg) return s;

    // If this replica is the original recipient of the RPC, respond.
    auto p = (std::pair<const ExecSqlRequest *, ExecSqlResponse *> *)arg;
    const ExecSqlRequest &request = *(p->first);
    ExecSqlResponse *response = p->second;

    if (!rows.empty()) {
      // Fill metadata info
      auto file_descriptor = tmp_pool->FindProtoFile(rows[0]->GetDescriptor()->name());
      google::protobuf::FileDescriptorSet* file_desc_set = new google::protobuf::FileDescriptorSet();
      file_descriptor->CopyTo(file_desc_set->add_file());
      response->set_allocated_descriptors(file_desc_set);
      for (size_t i = 0; i < rows.size(); ++i) {
        response->add_rows()->PackFrom(*rows[i], std::string());
        if (request.include_debug_strings())
          response->add_debug_strings(rows[i]->ShortDebugString());
      }
    }
    return OkStatus();
  }
}

}  // namespace sfdb
