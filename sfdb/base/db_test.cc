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

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/text_format.h"
#include "gtest/gtest.h"
#include "sfdb/base/vars.h"
#include "sfdb/proto/pool.h"

namespace sfdb {
namespace {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::TextFormat;

TEST(DbTest, CreateAndDropTable) {
  ProtoPool pool;
  const std::vector<std::pair<std::string, FieldDescriptor::Type>> empty;
  BuiltIns vars;
  Db db("foo", &vars);

  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_EQ("foo", db.name);
    EXPECT_FALSE(db.FindTable("bar"));
    EXPECT_EQ(0, db.tables.size());
  }

  {
    ::absl::WriterMutexLock lock(&db.mu);

    std::unique_ptr<ProtoPool> bar_pool = pool.Branch();
    const Descriptor *bar_type =
        bar_pool->CreateProtoClass("Bar", empty).ValueOrDie();
    Table *bar = db.PutTable("bar", std::move(bar_pool), bar_type);
    EXPECT_TRUE(bar);
    EXPECT_EQ("bar", bar->name);
    EXPECT_EQ(bar_type, bar->type);
    EXPECT_EQ(1, db.tables.size());

    std::unique_ptr<ProtoPool> baz_pool = pool.Branch();
    const Descriptor *baz_type =
        baz_pool->CreateProtoClass("Baz", empty).ValueOrDie();
    Table *baz = db.PutTable("baz", std::move(baz_pool), baz_type);
    EXPECT_TRUE(baz);
    EXPECT_EQ("baz", baz->name);
    EXPECT_EQ(baz_type, baz->type);
    EXPECT_EQ(2, db.tables.size());

    db.DropTable("bar");
    bar = nullptr;
    EXPECT_EQ(1, db.tables.size());

    db.DropTable("qux");  // non-existent table
    EXPECT_EQ(1, db.tables.size());

    db.DropTable("baz");
    baz = nullptr;
    EXPECT_EQ(0, db.tables.size());
  }
}

TEST(DbTest, Indices) {
  // Create database.
  ProtoPool pool;
  BuiltIns vars;
  Db db("db", &vars);
  ::absl::WriterMutexLock lock(&db.mu);

  // Create a table called Wine.
  std::unique_ptr<ProtoPool> wine_pool = pool.Branch();
  const std::vector<std::pair<std::string, FieldDescriptor::Type>> columns = {
      {"location", FieldDescriptor::TYPE_STRING},
      {"year", FieldDescriptor::TYPE_INT32},
      {"varietal", FieldDescriptor::TYPE_STRING},
  };
  const Descriptor *wine_type =
      wine_pool->CreateProtoClass("Wine", columns).ValueOrDie();
  Table *wine = db.PutTable("Wine", std::move(wine_pool), wine_type);
  EXPECT_TRUE(wine);
  EXPECT_TRUE(wine->indices.empty());

  // Create an index on year with ties broken by varietal.
  std::vector<const ::google::protobuf::FieldDescriptor *> index_columns = {
      wine->type->FindFieldByName("year"),
      wine->type->FindFieldByName("varietal"),
  };
  TableIndex *index = db.PutIndex(wine, "Index", std::move(index_columns));
  EXPECT_TRUE(index);
  EXPECT_EQ(index, db.FindIndex("Index"));
  EXPECT_EQ(index, wine->indices["Index"]);

  // Add a few rows.
  std::unique_ptr<Message> cf = wine->pool->NewMessage(wine->type);
  std::unique_ptr<Message> cb = wine->pool->NewMessage(wine->type);
  std::unique_ptr<Message> pt = wine->pool->NewMessage(wine->type);
  ASSERT_TRUE(TextFormat::ParseFromString(
      "location: 'Saratoga' year: 2014 varietal: 'Cabernet Franc'", cf.get()));
  ASSERT_TRUE(TextFormat::ParseFromString(
      "location: 'Loire' year: 2001 varietal: 'Chenin Blanc'", cb.get()));
  ASSERT_TRUE(TextFormat::ParseFromString(
      "location: 'South Africa' year: 2001 varietal: 'Pinotage'", pt.get()));
  const Message *const cf_ptr = cf.get();
  const Message *const cb_ptr = cb.get();
  const Message *const pt_ptr = pt.get();
  wine->Insert(std::move(cf));
  wine->Insert(std::move(cb));
  wine->Insert(std::move(pt));

  // Read them back in index order.
  ASSERT_EQ(3, index->tree.size());
  auto i = index->tree.begin();
  EXPECT_EQ(cb_ptr, i++->first);
  EXPECT_EQ(pt_ptr, i++->first);
  EXPECT_EQ(cf_ptr, i++->first);
  EXPECT_EQ(index->tree.end(), i);
}

}  // namespace
}  // namespace sfdb
