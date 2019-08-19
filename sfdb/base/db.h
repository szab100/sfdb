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
#ifndef SFDB_BASE_DB_H_
#define SFDB_BASE_DB_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/base/vars.h"
#include "sfdb/proto/pool.h"

namespace sfdb {

class TableIndex;

// A database table.
//
// Not thread-safe.
struct Table {
  const std::string name;
  std::unique_ptr<ProtoPool> pool;  // A child of the Db's |pool|.
  const ::google::protobuf::Descriptor *const type;  // Owned by |pool|
  std::vector<std::unique_ptr<::google::protobuf::Message>> rows;  // Of type |type|
  std::map<std::string, TableIndex*> indices;

  // |pool| must own |type|
  Table(::absl::string_view name, std::unique_ptr<ProtoPool> &&pool,
        const ::google::protobuf::Descriptor *type) :
      name(name), pool(std::move(pool)), type(type) {}

  ~Table() = default;
  Table(const Table&) = delete;
  Table(Table&&) = delete;
  Table &operator=(const Table&) = delete;
  Table &operator=(Table&&) = delete;

  // Appends a row and updates all indices.
  void Insert(std::unique_ptr<::google::protobuf::Message> &&row);
};

// An index over a database table.
//
// Not thread-safe.
class TableIndex {
public:
  // Comparator for sorting rows according to |columns|.
  // Immutable.
  struct Comparator {
    const TableIndex *const index;
    bool operator()(
        const std::pair<const ::google::protobuf::Message*, int> &apair,
        const std::pair<const ::google::protobuf::Message*, int> &bpair) const;
  };

  Table *const t;
  const std::string name;
  const std::vector<const ::google::protobuf::FieldDescriptor*> columns;

  using TableIndexTree = std::set<std::pair<const ::google::protobuf::Message*, int>, Comparator>;
  TableIndexTree tree;

  TableIndex(Table *t, ::absl::string_view name,
             std::vector<const ::google::protobuf::FieldDescriptor*> &&columns) :
      t(t), name(name), columns(std::move(columns)), tree(Comparator{this}) {}

  ~TableIndex() = default;
  TableIndex(const TableIndex&) = delete;
  TableIndex(TableIndex&&) = delete;
  TableIndex &operator=(const TableIndex&) = delete;
  TableIndex &operator=(TableIndex&&) = delete;
};

// A SQL database.
// Has a name. Contains tables and indices.
//
// Not thread-safe. Use |mu| to lock the whole Db.
struct Db {
  mutable ::absl::Mutex mu;
  const std::string name;
  std::unique_ptr<ProtoPool> pool;  // thread-safe on its own
  std::map<std::string, std::unique_ptr<Table>> tables GUARDED_BY(mu);
  std::unique_ptr<Vars> vars GUARDED_BY(mu);
  std::map<std::string, std::unique_ptr<TableIndex>> table_indices GUARDED_BY(mu);

  Db(::absl::string_view name, Vars *root_vars);

  ~Db() = default;
  Db(const Db&) = delete;
  Db(const Db&&) = delete;
  Db &operator=(const Db&) = delete;
  Db &operator=(const Db&&) = delete;

  // These functions don't touch the |pool|. Deal with it separately.
  Table *FindTable(::absl::string_view name) const SHARED_LOCKS_REQUIRED(mu);
  Table *PutTable(::absl::string_view name, std::unique_ptr<ProtoPool> &&pool,
                  const ::google::protobuf::Descriptor *type)
      EXCLUSIVE_LOCKS_REQUIRED(mu);
  bool DropTable(::absl::string_view name) EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Table indices.
  TableIndex *FindIndex(::absl::string_view index_name) const
      SHARED_LOCKS_REQUIRED(mu);
  TableIndex *PutIndex(Table *t, ::absl::string_view index_name,
                       std::vector<const ::google::protobuf::FieldDescriptor*> &&columns)
      EXCLUSIVE_LOCKS_REQUIRED(mu);
  bool DropIndex(::absl::string_view index_name) EXCLUSIVE_LOCKS_REQUIRED(mu);
};

}  // namespace sfdb

#endif  // SFDB_BASE_DB_H_
