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
#include "sfdb/proto/field_path.h"

#include "google/protobuf/descriptor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "sfdb/base/value.h"
#include "sfdb/testing/data.pb.h"
#include "util/proto/parse_text_proto.h"
#include "util/proto/matchers.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::google::protobuf::FieldDescriptor;
using ::testing::EqualsProto;
using ::util::IsInvalidArgument;
using ::util::IsNotFound;

TEST(ProtoFieldPathTest, Basic) {
  ProtoPool pool;
  auto pfp = [&pool](const char *path) {
    return ProtoFieldPath::Make(&pool, "sfdb.Data", path).ValueOrDie();
  };

  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, pfp("").type());
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, pfp("pts[0]").type());
  EXPECT_EQ(FieldDescriptor::TYPE_INT32, pfp("pts[0].x").type());
  EXPECT_EQ(FieldDescriptor::TYPE_INT32, pfp("pts[1].y").type());
  EXPECT_EQ(FieldDescriptor::TYPE_DOUBLE, pfp("pts[-1].weight").type());
  EXPECT_EQ(FieldDescriptor::TYPE_STRING, pfp("plot_title").type());
}

TEST(ProtoFieldPathTest, Errors) {
  ProtoPool pool;
  auto err = [&pool](const char *path) {
    return ProtoFieldPath::Make(&pool, "sfdb.Data", path).status();
  };

  EXPECT_TRUE(IsNotFound(err("blah")));
  EXPECT_TRUE(IsNotFound(err(".")));
  EXPECT_TRUE(IsNotFound(err("pts")));  // TODO: make this work
  EXPECT_TRUE(IsNotFound(err("pts.blah")));
  EXPECT_TRUE(IsNotFound(err("pts[0].blah")));
  EXPECT_TRUE(IsNotFound(err("pts[]")));  // TODO: ditto
  EXPECT_TRUE(IsNotFound(err("pts[+3]")));
  EXPECT_TRUE(IsNotFound(err("pts[x]")));  // TODO: ditto!! omg
  EXPECT_TRUE(IsNotFound(err("pts[0].x.y")));
  EXPECT_TRUE(IsNotFound(err("pts[0]..x")));
}

TEST(ProtoFieldPathTest, GetFrom) {
  const Data msg =  PARSE_TEST_PROTO(R"(
    pts: { x: 10 y: 20 weight: 1.0 }
    pts: { x: 13 y: 17 weight: -1.2 }
    pts: { x: 66 y: 99 weight: 1e-7 }
    plot_title: "Howdy")");

  auto get = [&msg](const char *path) {
    return ProtoFieldPath::Make(msg.GetDescriptor(), path).ValueOrDie()
        .GetFrom(msg).ValueOrDie();
  };

  EXPECT_EQ(Value::Int64(10), get("pts[0].x"));
  EXPECT_EQ(Value::Int64(17), get("pts[1].y"));
  EXPECT_EQ(Value::Double(1e-7), get("pts[-1].weight"));
  EXPECT_EQ(Value::String("Howdy"), get("plot_title"));
  EXPECT_THAT(*get("pts[-2]").msg, EqualsProto(msg.pts(1)));
  EXPECT_THAT(*get("").msg, EqualsProto(msg));

  auto err = [&msg](const char *path) {
    return ProtoFieldPath::Make(msg.GetDescriptor(), path).ValueOrDie()
        .GetFrom(msg).status();
  };

  EXPECT_TRUE(IsInvalidArgument(err("pts[3]")));
  EXPECT_TRUE(IsInvalidArgument(err("pts[-4]")));
}
}  // namespace
}  // namespace sfdb
