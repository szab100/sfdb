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

#include "server/brpc_sfdb_server.h"

#include "absl/memory/memory.h"
#include "server/brpc_sfdb_server_impl.h"
#include "server/common_types.h"
#include "sfdb/base/ast.h"
#include "sfdb/base/db.h"
#include "sfdb/base/vars.h"
#include "sfdb/engine/engine.h"
#include "sfdb/sql/parser.h"

namespace sfdb {
using google::protobuf::Message;
using util::Status;
using util::StatusOr;

BrpcSfdbServer::BrpcSfdbServer()
    : pimpl_(absl::make_unique<BrpcSfdbServerImpl>()) {
  built_in_vars_ = absl::make_unique<BuiltIns>();
  db_ = absl::make_unique<Db>("MAIN", built_in_vars_.get());
}

BrpcSfdbServer::~BrpcSfdbServer() = default;

bool BrpcSfdbServer::StartAndWait(const std::string &host, int port,
                                  const std::string &raft_targets) {
  bool res = pimpl_->Start(
      host, port, raft_targets,
      [this](const std::string &sql,
             ::sfdb::ExecSqlResponse *response) -> BraftExecSqlResult {
        StatusOr<std::unique_ptr<Ast>> ast_so = Parse(sql);

        if (!ast_so.ok())
          return BraftExecSqlResult(ast_so.status().CanonicalCode(),
                                    ast_so.status().error_message());

        std::unique_ptr<Ast> ast = std::move(ast_so.ValueOrDie());
        std::unique_ptr<ProtoPool> tmp_pool = db_->pool->Branch();
        if (ast->IsMutation()) {
          Status s = ExecuteWrite(std::move(ast), tmp_pool.get(), db_.get());
          if (s.ok()) {
            return BraftExecSqlResult(::util::error::OK, "");
          } else {
            return BraftExecSqlResult(s.CanonicalCode(), s.error_message());
          }
        } else {
          std::vector<std::unique_ptr<Message>> rows;
          Status s =
              ExecuteRead(std::move(ast), tmp_pool.get(), db_.get(), &rows);
          if (!s.ok())
            return BraftExecSqlResult(s.CanonicalCode(), s.error_message());

          if (!!response && !rows.empty()) {
            // Fill metadata info
            auto file_descriptor =
                tmp_pool->FindProtoFile(rows[0]->GetDescriptor()->name());

            if (!file_descriptor)
              return BraftExecSqlResult(::util::error::INTERNAL, "Descriptor not found");

            google::protobuf::FileDescriptorSet *file_desc_set =
                new google::protobuf::FileDescriptorSet();

            if (!file_desc_set)
              return BraftExecSqlResult(::util::error::INTERNAL, "Descriptor not found");

            file_descriptor->CopyTo(file_desc_set->add_file());
            response->set_allocated_descriptors(file_desc_set);
            for (size_t i = 0; i < rows.size(); ++i) {
              response->add_rows()->PackFrom(*rows[i], std::string());
            }
          }
          return BraftExecSqlResult(::util::error::OK, "");  // OkStatus();
        }
      });

  if (res) {
    pimpl_->WaitTillStopped();
    return true;
  } else {
    return false;
  }
}

bool BrpcSfdbServer::Stop() {
  pimpl_->Stop();
  return true;
}

}  // namespace sfdb
