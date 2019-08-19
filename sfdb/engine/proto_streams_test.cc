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
#include "sfdb/engine/proto_streams.h"

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "google/protobuf/empty.pb.h"
#include "google/protobuf/message.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "sfdb/base/db.h"
#include "sfdb/base/proto_stream.h"
#include "sfdb/testing/data.pb.h"
#include "util/proto/parse_text_proto.h"
#include "util/proto/matchers.h"
#include "util/task/canonical_errors.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::absl::make_unique;
using ::google::protobuf::Empty;
using ::google::protobuf::Message;
using ::testing::EqualsProto;
using ::util::InvalidArgumentError;
using ::util::IsInvalidArgument;
using ::util::StatusOr;

const Point &AsPoint(const Message &msg) {
  return *reinterpret_cast<const Point*>(&msg);
}

// A predicate that returns true for points with even y-coordinates.
StatusOr<bool> IsEvenY(const Message &msg) {
  return AsPoint(msg).y() % 2 == 0;
}

// A predicate that returns true for points with even y-coordinates.
StatusOr<bool> IsOddY(const Message &msg) {
  return AsPoint(msg).y() % 2 != 0;
}

// A predicate that fails on points with x=3.
StatusOr<bool> FaultyPredicate(const Message &msg) {
  if (AsPoint(msg).x() == 3) return InvalidArgumentError("x cannot be 3");
  return true;
}

// A funtion that converts a point into a Data proto with plot_title set to
// (x,y).
StatusOr<std::unique_ptr<Data>> StringifyPoint(const Message &msg) {
  const Point &p = AsPoint(msg);
  std::unique_ptr<Data> out(new Data);
  out->set_plot_title(StrCat("(", p.x(), ",", p.y(), ")"));
  return out;
}

TEST(ProtoStreamTest, TableProtoStream) {
  const Point a = PARSE_TEST_PROTO("x: 1 y: 2");
  const Point b = PARSE_TEST_PROTO("x: 3 y: 4");
  const Point c = PARSE_TEST_PROTO("x: 5 y: 6");
  ProtoPool pool;
  Table t("Points", pool.Branch(), a.GetDescriptor());
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows[0]->CopyFrom(a);
  t.rows[1]->CopyFrom(b);
  t.rows[2]->CopyFrom(c);

  TableProtoStream tps(&t);
  EXPECT_FALSE(tps.Done());
  EXPECT_TRUE(tps.ok());
  EXPECT_THAT(*tps, EqualsProto(a));
  EXPECT_EQ(0, tps.GetIndexInTable());
  ++tps;
  EXPECT_FALSE(tps.Done());
  EXPECT_TRUE(tps.ok());
  EXPECT_THAT(*tps, EqualsProto(b));
  EXPECT_EQ(1, tps.GetIndexInTable());
  ++tps;
  EXPECT_FALSE(tps.Done());
  EXPECT_TRUE(tps.ok());
  EXPECT_THAT(*tps, EqualsProto(c));
  EXPECT_EQ(2, tps.GetIndexInTable());
  ++tps;
  EXPECT_TRUE(tps.Done());
  EXPECT_TRUE(tps.ok());
}

TEST(ProtoStreamTest, FilterProtoStream) {
  const Point a = PARSE_TEST_PROTO("x: 1 y: 7");
  const Point b = PARSE_TEST_PROTO("x: 2 y: 8");
  const Point c = PARSE_TEST_PROTO("x: 3 y: 9");
  ProtoPool pool;
  Table t("Points", pool.Branch(), a.GetDescriptor());
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows[0]->CopyFrom(a);
  t.rows[1]->CopyFrom(b);
  t.rows[2]->CopyFrom(c);

  {
    std::unique_ptr<TableProtoStream> tps(new TableProtoStream(&t));
    FilterProtoStream fps(std::move(tps), &IsEvenY);
    EXPECT_FALSE(fps.Done());
    EXPECT_TRUE(fps.ok());
    EXPECT_THAT(*fps, EqualsProto(b));
    ++fps;
    EXPECT_TRUE(fps.Done());
    EXPECT_TRUE(fps.ok());
  }

  {
    std::unique_ptr<TableProtoStream> tps(new TableProtoStream(&t));
    FilterProtoStream fps(std::move(tps), &IsOddY);
    EXPECT_FALSE(fps.Done());
    EXPECT_TRUE(fps.ok());
    EXPECT_THAT(*fps, EqualsProto(a));
    ++fps;
    EXPECT_FALSE(fps.Done());
    EXPECT_TRUE(fps.ok());
    EXPECT_THAT(*fps, EqualsProto(c));
    ++fps;
    EXPECT_TRUE(fps.Done());
    EXPECT_TRUE(fps.ok());
  }

  {
    std::unique_ptr<TableProtoStream> tps(new TableProtoStream(&t));
    FilterProtoStream fps(std::move(tps), &FaultyPredicate);
    EXPECT_FALSE(fps.Done());
    EXPECT_TRUE(fps.ok());
    EXPECT_THAT(*fps, EqualsProto(a));
    ++fps;
    EXPECT_FALSE(fps.Done());
    EXPECT_TRUE(fps.ok());
    EXPECT_THAT(*fps, EqualsProto(b));
    ++fps;
    EXPECT_FALSE(fps.ok());
    EXPECT_TRUE(IsInvalidArgument(fps.status()));
  }
}

TEST(ProtoStreamTest, MapProtoStream) {
  const Point a = PARSE_TEST_PROTO("x: 1 y: 7");
  const Point b = PARSE_TEST_PROTO("x: 2 y: 8");
  const Point c = PARSE_TEST_PROTO("x: 3 y: 9");
  ProtoPool pool;
  Table t("Points", pool.Branch(), a.GetDescriptor());
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows.push_back(std::unique_ptr<Point>(new Point));
  t.rows[0]->CopyFrom(a);
  t.rows[1]->CopyFrom(b);
  t.rows[2]->CopyFrom(c);

  const Data out_a = PARSE_TEST_PROTO("plot_title: '(1,7)'");
  const Data out_b = PARSE_TEST_PROTO("plot_title: '(2,8)'");
  const Data out_c = PARSE_TEST_PROTO("plot_title: '(3,9)'");

  std::unique_ptr<TableProtoStream> tps(new TableProtoStream(&t));
  MapProtoStream mps(std::move(tps), out_a.GetDescriptor(), &StringifyPoint);
  EXPECT_FALSE(mps.Done());
  EXPECT_TRUE(mps.ok());
  EXPECT_THAT(*mps, EqualsProto(out_a));
  EXPECT_THAT(*++mps, EqualsProto(out_b));
  EXPECT_THAT(*++mps, EqualsProto(out_c));
  EXPECT_TRUE((++mps).ok());
  EXPECT_TRUE(mps.Done());
}

TEST(ProtoStreamTest, TmpTableProtoStream_OnePoint) {
  const Point a = PARSE_TEST_PROTO("x: 1 y: 2");
  std::vector<std::unique_ptr<Message>> rows;
  rows.emplace_back(new Point(a));
  TmpTableProtoStream ttps(std::move(rows));
  EXPECT_FALSE(ttps.Done());
  EXPECT_TRUE(ttps.ok());
  EXPECT_THAT(*ttps, EqualsProto(a));
  EXPECT_TRUE((++ttps).ok());
  EXPECT_TRUE(ttps.Done());
}

TEST(ProtoStreamTest, TmpTableProtoStream_TwoEmpties){
  std::vector<std::unique_ptr<Message>> rows;
  rows.emplace_back(new Empty);
  rows.emplace_back(new Empty);
  TmpTableProtoStream ttps(std::move(rows));
  EXPECT_FALSE(ttps.Done());
  EXPECT_TRUE(ttps.ok());
  EXPECT_THAT(*ttps, EqualsProto(Empty::default_instance()));
  EXPECT_THAT(*(++ttps), EqualsProto(Empty::default_instance()));
  EXPECT_TRUE((++ttps).ok());
  EXPECT_TRUE(ttps.Done());
}

TEST(ProtoStreamTest, TableIndexProtoStream) {
  const Point a = PARSE_TEST_PROTO("x: 1 y: 2");
  const Point b = PARSE_TEST_PROTO("x: 3 y: 6");
  const Point c = PARSE_TEST_PROTO("x: 5 y: 4");
  ProtoPool pool;
  Table t("Points", pool.Branch(), a.GetDescriptor());
  TableIndex ti(&t, "ByY", {t.type->FindFieldByName("y")});
  t.indices[ti.name] = &ti;
  t.Insert(make_unique<Point>(a));
  t.Insert(make_unique<Point>(b));
  t.Insert(make_unique<Point>(c));

  TableIndexProtoStream tips(ti, {&a, false}, {&b, true});
  EXPECT_FALSE(tips.Done());
  EXPECT_TRUE(tips.ok());
  EXPECT_THAT(*tips, EqualsProto(c));
  EXPECT_EQ(2, tips.GetIndexInTable());
  ++tips;
  EXPECT_FALSE(tips.Done());
  EXPECT_TRUE(tips.ok());
  EXPECT_THAT(*tips, EqualsProto(b));
  EXPECT_EQ(1, tips.GetIndexInTable());
  ++tips;
  EXPECT_TRUE(tips.Done());
  EXPECT_TRUE(tips.ok());
}

}  // namespace
}  // namespace sfdb
