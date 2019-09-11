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
#include "sfdb/engine/engine.h"

#include <memory>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "google/protobuf/message.h"
#include "sfdb/base/vars.h"
#include "sfdb/proto/pool.h"
#include "sfdb/sql/parser.h"
#include "util/task/status.h"
#include "util/task/status_matchers.h"
#include "util/task/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace sfdb {
namespace {

using ::google::protobuf::Message;
using ::util::Status;

void Go(
    const char *sql, ProtoPool *pool,
    std::vector<std::unique_ptr<Message>> *rows) {
  BuiltIns vars;
  Db db("Test", &vars);
  std::unique_ptr<Ast> ast = Parse(sql).ValueOrDie();
  ASSERT_OK(Execute(std::move(ast), pool, &db, rows));
}

TEST(EngineTest, ExecuteSingleRow) {
  ProtoPool pool;
  std::vector<std::unique_ptr<Message>> rows;
  Go("SELECT 0;", &pool, &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("_1: 0\n", rows[0]->DebugString());
}

TEST(EngineTest, ExecuteSingleRow_MultiValue) {
  ProtoPool pool;
  std::vector<std::unique_ptr<Message>> rows;
  Go("SELECT 1, 3.14, 'dude';", &pool, &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("_1: 1\n_2: 3.14\n_3: \"dude\"\n", rows[0]->DebugString());
}

TEST(EngineTest, ExecuteSingleRow_Named) {
  ProtoPool pool;
  std::vector<std::unique_ptr<Message>> rows;
  Go("SELECT -1 AS neg, 3.14 AS pi;", &pool, &rows);
  ASSERT_EQ(1, rows.size());
  ASSERT_EQ("neg: -1\npi: 3.14\n", rows[0]->DebugString());
}

TEST(EngineTest, ExecuteSingleRow_Func) {
  ProtoPool pool;
  std::vector<std::unique_ptr<Message>> rows;
  Go("SELECT LEN('hello');", &pool, &rows);
  ASSERT_EQ("_1: 5\n", rows[0]->DebugString());
}

TEST(EngineTest, MiniE2e) {
  ProtoPool pool;
  BuiltIns vars;
  Db db("Test", &vars);
  std::vector<std::unique_ptr<Message>> rows;
  ASSERT_OK(Execute(Parse(
      "CREATE TABLE People (name string, age int64);")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "INSERT INTO People (name, age) VALUES ('joe', 13);")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "INSERT INTO People (name, age) VALUES ('bob', 16);")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "SELECT name FROM People WHERE age % 2 = 0;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("_1: \"bob\"", rows[0]->ShortDebugString());
  EXPECT_OK(Execute(Parse(
      "UPDATE People SET age = 17 WHERE name = 'bob';")
      .ValueOrDie(), &pool, &db, &rows));
  rows.clear();
  ASSERT_OK(Execute(Parse(
      "SELECT name FROM People WHERE age % 2;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("_1: \"joe\"", rows[0]->ShortDebugString());
  EXPECT_EQ("_1: \"bob\"", rows[1]->ShortDebugString());
  rows.clear();
  ASSERT_OK(Execute(Parse(
      "CREATE INDEX ByName ON People (name);")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "UPDATE People SET age = 18 WHERE name = 'bob';;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "SELECT name FROM People WHERE age = 18;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("_1: \"bob\"", rows[0]->ShortDebugString());
  rows.clear();
  ASSERT_OK(Execute(Parse(
      "DROP TABLE People;")
      .ValueOrDie(), &pool, &db, &rows));
}

TEST(EngineTest, CreateAndDrop) {
  ProtoPool pool;
  BuiltIns vars;
  Db db("Test", &vars);
  std::vector<std::unique_ptr<Message>> rows;

  // Create tables.
  ASSERT_OK(Execute(Parse(
      "CREATE TABLE TableA (s string);").ValueOrDie(),
      &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "CREATE TABLE TableB (i int64, d double);").ValueOrDie(),
      &pool, &db, &rows));
  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_EQ("TableA", db.FindTable("TableA")->name);
    EXPECT_EQ("TableB", db.FindTable("TableB")->name);
  }

  // Create indices.
  ASSERT_OK(Execute(Parse(
      "CREATE INDEX IndexI ON TableA (s);").ValueOrDie(),
      &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "CREATE INDEX IndexJ ON TableB (d,);").ValueOrDie(),
      &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "CREATE INDEX IndexK ON TableB (d,i);").ValueOrDie(),
      &pool, &db, &rows));
  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_EQ("IndexI", db.FindIndex("IndexI")->name);
    EXPECT_EQ("TableA", db.FindIndex("IndexI")->t->name);
    EXPECT_EQ("IndexJ", db.FindIndex("IndexJ")->name);
    EXPECT_EQ("TableB", db.FindIndex("IndexJ")->t->name);
    EXPECT_EQ("IndexK", db.FindIndex("IndexK")->name);
    EXPECT_EQ("TableB", db.FindIndex("IndexK")->t->name);
  }

  // Drop an index.
  ASSERT_OK(Execute(Parse(
      "DROP INDEX IndexI;").ValueOrDie(),
      &pool, &db, &rows));
  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_FALSE(db.FindIndex("IndexI"));
    EXPECT_TRUE(db.FindIndex("IndexJ"));
    EXPECT_TRUE(db.FindIndex("IndexK"));
  }

  // Drop TableA.
  ASSERT_OK(Execute(Parse(
      "DROP TABLE TableA;").ValueOrDie(),
      &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "CREATE TABLE TableA (s string);").ValueOrDie(),
      &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "DROP TABLE TableA;").ValueOrDie(),
      &pool, &db, &rows));
  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_TRUE(db.FindIndex("IndexJ"));
    EXPECT_TRUE(db.FindIndex("IndexK"));
  }

  // Drop TableB.
  ASSERT_OK(Execute(Parse(
      "DROP TABLE TableB;").ValueOrDie(),
      &pool, &db, &rows));

  // Reamaining indices should be gone now.
  {
    ::absl::ReaderMutexLock lock(&db.mu);
    EXPECT_FALSE(db.FindIndex("IndexJ"));
    EXPECT_FALSE(db.FindIndex("IndexK"));
  }
}

TEST(EngineTest, ShowTables) {
  ProtoPool pool;
  BuiltIns vars;
  Db db("Test", &vars);
  std::vector<std::unique_ptr<Message>> rows;
  ASSERT_OK(Execute(Parse(
      "SHOW TABLES;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(0, rows.size());

  ASSERT_OK(Execute(Parse(
      "CREATE TABLE People (name string, age int64);")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_OK(Execute(Parse(
      "CREATE TABLE Addresses (address string, contact_id int64);")
      .ValueOrDie(), &pool, &db, &rows));

  ASSERT_OK(Execute(Parse(
      "SHOW TABLES;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(2, rows.size());
  // TODO: this check relies on the order of records, which might change
  EXPECT_EQ("table_name: \"People\"", rows[1]->ShortDebugString());
  EXPECT_EQ("table_name: \"Addresses\"", rows[0]->ShortDebugString());

  ASSERT_OK(Execute(Parse(
      "DROP TABLE People;")
      .ValueOrDie(), &pool, &db, &rows));

  rows.clear();

  ASSERT_OK(Execute(Parse(
      "SHOW TABLES;")
      .ValueOrDie(), &pool, &db, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ("table_name: \"Addresses\"", rows[0]->ShortDebugString());
}

}  // namespace
}  // namespace sfdb
