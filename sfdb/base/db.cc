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
#include "util/task/statusor.h"

namespace sfdb {
namespace {

template<class T>
inline int Cmp(const T &a, const T &b) { return a < b ? -1 : (a == b ? 0 : 1); }

using ::absl::make_unique;
using ::absl::string_view;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;
using ::util::StatusOr;

const std::string kTableListProtoName = "__DB_TABLE_LIST__";
const std::string kTableDescProtoName = "__DB_TABLE_DESC__";

// Utility functions to build descriptors for internal DB tables.

const ::google::protobuf::Descriptor *BuildTableListDescriptor(ProtoPool *p) {
  const std::vector<std::pair<std::string, FieldDescriptor::Type>> fields = {
      {"table_name", FieldDescriptor::TYPE_STRING},
  };

  return p->CreateProtoClass(kTableListProtoName, fields).ValueOrDie();
}

const ::google::protobuf::Descriptor *BuildTableDescDescriptor(ProtoPool *p) {
  const std::vector<std::pair<std::string, FieldDescriptor::Type>> fields = {
      {"field_name", FieldDescriptor::TYPE_STRING},
      {"field_type", FieldDescriptor::TYPE_STRING},
  };

  return p->CreateProtoClass(kTableDescProtoName, fields).ValueOrDie();
}

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
  auto new_table_ptr = (tables[name_str] = make_unique<Table>(
      name, std::move(pool), type)).get();
  scheme_changed_ = true;
  return new_table_ptr;
}

void Db::UpdateTableList() const {
  table_list_->rows.clear();
  const auto field = table_list_->type->FindFieldByNumber(1);
  CHECK(!!field);

  for (const auto& i : tables) {
    auto row = table_list_->pool->NewMessage(table_list_->type);
    auto reflection = row->GetReflection();
    reflection->SetString(row.get(), field, i.first);

    table_list_->rows.emplace_back(std::move(row));
  }
}

void Db::CreateTableListTable() const {
  std::unique_ptr<ProtoPool> table_pool = pool->Branch();
  auto table_list_descriptor = BuildTableListDescriptor(pool.get());
  table_list_ = absl::make_unique<Table>(kTableListProtoName, std::move(table_pool),
    table_list_descriptor);
}

void Db::UpdateTableDescritption(const Table* table) {
  Table *t = nullptr;

  // Find table which contains descritption, if it is not found - create new.
  auto it = table_descs_.find(table->name);
  if (it == table_descs_.end()) {
    std::unique_ptr<ProtoPool> table_pool = pool->Branch();
    auto table_description_descriptor = BuildTableDescDescriptor(pool.get());
    t = (table_descs_[table->name] = absl::make_unique<Table>(absl::StrCat(kTableListProtoName, table->name),
      std::move(table_pool), table_description_descriptor)).get();
  } else {
    t = it->second.get();
  }

  // Fill table with descriptiot of requested table.

  int field_count = table->type->field_count();

  // Cache fields from description table.
  const auto name_field = t->type->FindFieldByNumber(1);
  CHECK(!!name_field);
  const auto type_field = t->type->FindFieldByNumber(2);
  CHECK(!!type_field);

  t->rows.clear();
  for (int i = 0; i < field_count; ++i) {
    auto row = t->pool->NewMessage(t->type);
    auto reflection = row->GetReflection();
    reflection->SetString(row.get(), name_field, table->type->field(i)->name());
    reflection->SetString(row.get(), type_field, table->type->field(i)->type_name());

    t->rows.push_back(std::move(row));
  }
}


void Db::RemoveTableDescritption(const std::string& table_name) {
  auto it = table_descs_.find(table_name);
  CHECK(it != table_descs_.end());
}

const ::google::protobuf::Descriptor* Db::GetTableListTableType() const {
  return table_list_->type;
}
const Table *Db::GetTableList() const {
  if (scheme_changed_) {
    UpdateTableList();
    scheme_changed_ = false;
  }

  return table_list_.get();
}

Db::Db(string_view name, Vars *root_vars)
    : name(name),
      pool(new ProtoPool),
      vars(root_vars->Branch()),
      scheme_changed_(false) {
  // Cretae DB internal tables
  CreateTableListTable();
}

bool Db::DropTable(string_view name) {
  const std::string name_str(name);
  Table *t = FindTable(name_str);
  if (!t) return false;

  // Drop all of the table's indices first.
  while (!t->indices.empty()) CHECK(DropIndex(t->indices.begin()->first));

  CHECK(tables.erase(std::string(name)));
  scheme_changed_ = true;
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
