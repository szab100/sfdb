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
#include "sfdb/base/db.h"

#include <map>
#include <string>
#include <vector>

#include "absl/memory/memory.h"
#include "glog/logging.h"

namespace sfdb {
namespace {

template<class T>
inline int Cmp(const T &a, const T &b) { return a < b ? -1 : (a == b ? 0 : 1); }

using ::absl::make_unique;
using ::absl::string_view;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;

}  // namespace

void Table::Insert(std::unique_ptr<Message> &&row) {
  rows.push_back(std::move(row));
  for (auto &i : indices) {
    TableIndex *index = i.second;
    index->tree.insert({rows.back().get(), rows.size() - 1});
  }
}

Table *Db::FindTable(string_view name) const {
  auto i = tables.find(std::string(name));
  return i != tables.end() ? i->second.get() : nullptr;
}

Table *Db::PutTable(string_view name, std::unique_ptr<ProtoPool> &&pool,
                    const Descriptor *type) {
  const std::string name_str(name);
  CHECK(!tables.count(name_str));
  return (tables[name_str] = make_unique<Table>(
      name, std::move(pool), type)).get();
}

Db::Db(string_view name, Vars *root_vars) :
    name(name), pool(new ProtoPool), vars(root_vars->Branch()) {
}

bool Db::DropTable(string_view name) {
  const std::string name_str(name);
  Table *t = FindTable(name_str);
  if (!t) return false;

  // Drop all of the table's indices first.
  while (!t->indices.empty()) CHECK(DropIndex(t->indices.begin()->first));

  CHECK(tables.erase(std::string(name)));
  return true;
}

TableIndex *Db::FindIndex(string_view index_name) const {
  auto i = table_indices.find(std::string(index_name));
  return i != table_indices.end() ? i->second.get() : nullptr;
}

TableIndex *Db::PutIndex(
    Table *t, string_view index_name,
    std::vector<const FieldDescriptor*> &&columns) {
  const std::string index_name_str(index_name);
  CHECK(!table_indices.count(index_name_str));
  TableIndex *index = (
      table_indices[index_name_str] = make_unique<TableIndex>(
          t, index_name, std::move(columns)
      )).get();
  t->indices[index_name_str] = index;

  // Index the current contents of the table.
  for (size_t i = 0; i < t->rows.size(); ++i)
    index->tree.insert({t->rows[i].get(), i});

  return index;
}

bool Db::DropIndex(::absl::string_view index_name) {
  const std::string index_name_str(index_name);
  TableIndex *i = FindIndex(index_name_str);
  if (!i) return false;
  CHECK(i->t->indices.erase(index_name_str));
  CHECK(table_indices.erase(index_name_str));
  return true;
}

bool TableIndex::Comparator::operator()(
    const std::pair<const Message*, int> &apair,
    const std::pair<const Message*, int> &bpair) const {
  const Message *a = apair.first;
  const Message *b = bpair.first;
  const Reflection *aref = a->GetReflection();
  const Reflection *bref = b->GetReflection();
  for (const FieldDescriptor *fd : index->columns) {
    CHECK(!fd->is_repeated());
    int sign;
    switch (fd->cpp_type()) {
      case FieldDescriptor::CPPTYPE_INT32:
        sign = Cmp(aref->GetInt32(*a, fd), bref->GetInt32(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_INT64:
        sign = Cmp(aref->GetInt64(*a, fd), bref->GetInt64(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_UINT32:
        sign = Cmp(aref->GetUInt32(*a, fd), bref->GetUInt32(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_UINT64:
        sign = Cmp(aref->GetUInt64(*a, fd), bref->GetUInt64(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_DOUBLE:
        sign = Cmp(aref->GetDouble(*a, fd), bref->GetDouble(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_FLOAT:
        sign = Cmp(aref->GetFloat(*a, fd), bref->GetFloat(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_BOOL:
        sign = Cmp(aref->GetBool(*a, fd), bref->GetBool(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_ENUM:
        sign = Cmp(aref->GetEnumValue(*a, fd), bref->GetEnumValue(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_STRING:
        sign = Cmp(aref->GetString(*a, fd), bref->GetString(*b, fd));
        break;
      case FieldDescriptor::CPPTYPE_MESSAGE:
        LOG(FATAL) << "Message-valued field in a TableIndex.";
    }
    if (sign) return sign < 0;
  }
  return apair.second < bpair.second;
}

}  // namespace
