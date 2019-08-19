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
#include "sfdb/base/value.h"

#include <memory>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "gtest/gtest.h"
#include "sfdb/testing/data.pb.h"
#include "util/proto/parse_text_proto.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::google::protobuf::Message;

std::unique_ptr<Message> Msg(const char *str) {
   return std::unique_ptr<Message>(new Point(PARSE_TEST_PROTO(str)));
}

TEST(ValueTest, ToString) {
  EXPECT_EQ("TRUE", Value::Bool(true).ToString());
  EXPECT_EQ("FALSE", Value::Bool(false).ToString());
  EXPECT_EQ("0", Value::Int64(0).ToString());
  EXPECT_EQ("1", Value::Int64(1).ToString());
  EXPECT_EQ("-1", Value::Int64(-1).ToString());
  EXPECT_EQ("1234567890", Value::Int64(1234567890).ToString());
  EXPECT_EQ("\"hi\"", Value::String("hi").ToString());
  EXPECT_EQ("\"\\\"\"", Value::String("\"").ToString());
  EXPECT_EQ("Proto<sfdb.Point>{x: 1 y: 2}",
            Value::Message(Msg("x:1 y:2")).ToString());
}

TEST(ValueTest, CastTo) {
  const auto TYPE_BOOL = ::google::protobuf::FieldDescriptor::TYPE_BOOL;
  const auto TYPE_INT64 = ::google::protobuf::FieldDescriptor::TYPE_INT64;
  const auto TYPE_DOUBLE = ::google::protobuf::FieldDescriptor::TYPE_DOUBLE;
  const auto TYPE_STRING = ::google::protobuf::FieldDescriptor::TYPE_STRING;

  // bool to bool
  EXPECT_EQ(Value::Bool(true),
            Value::Bool(true).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(false),
            Value::Bool(false).CastTo(TYPE_BOOL).ValueOrDie());

  // bool to int64
  EXPECT_EQ(Value::Int64(1),
            Value::Bool(true).CastTo(TYPE_INT64).ValueOrDie());
  EXPECT_EQ(Value::Int64(0),
            Value::Bool(false).CastTo(TYPE_INT64).ValueOrDie());

  // bool to double
  EXPECT_EQ(Value::Double(1.0),
            Value::Bool(true).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(0.0),
            Value::Bool(false).CastTo(TYPE_DOUBLE).ValueOrDie());

  // bool to string
  EXPECT_EQ(Value::String("1"),
            Value::Bool(true).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("0"),
            Value::Bool(false).CastTo(TYPE_STRING).ValueOrDie());

  // int64 to bool
  EXPECT_EQ(Value::Bool(false),
            Value::Int64(0).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Int64(1).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Int64(-1).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Int64(12345).CastTo(TYPE_BOOL).ValueOrDie());

  // int64 to int64
  EXPECT_EQ(Value::Int64(0),
            Value::Int64(0).CastTo(TYPE_INT64).ValueOrDie());
  EXPECT_EQ(Value::Int64(1),
            Value::Int64(1).CastTo(TYPE_INT64).ValueOrDie());
  EXPECT_EQ(Value::Int64(-1),
            Value::Int64(-1).CastTo(TYPE_INT64).ValueOrDie());
  EXPECT_EQ(Value::Int64(12345),
            Value::Int64(12345).CastTo(TYPE_INT64).ValueOrDie());

  // int64 to double
  EXPECT_EQ(Value::Double(0.0),
            Value::Int64(0).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(1.0),
            Value::Int64(1).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(-1.0),
            Value::Int64(-1).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(12345.0),
            Value::Int64(12345).CastTo(TYPE_DOUBLE).ValueOrDie());

  // int64 to string
  EXPECT_EQ(Value::String("0"),
            Value::Int64(0).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("1"),
            Value::Int64(1).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("-1"),
            Value::Int64(-1).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("12345"),
            Value::Int64(12345).CastTo(TYPE_STRING).ValueOrDie());

  // double to bool
  EXPECT_EQ(Value::Bool(false),
            Value::Double(0.0).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Double(1.0).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Double(1e-7).CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::Double(-1.2e34).CastTo(TYPE_BOOL).ValueOrDie());

  // double to int64
  EXPECT_TRUE(::util::IsInvalidArgument(
      Value::Double(0.0).CastTo(TYPE_INT64).status()));

  // double to double
  EXPECT_EQ(Value::Double(0.0),
            Value::Double(0.0).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(1.0),
            Value::Double(1.0).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(1e-7),
            Value::Double(1e-7).CastTo(TYPE_DOUBLE).ValueOrDie());
  EXPECT_EQ(Value::Double(-1.2e34),
            Value::Double(-1.2e34).CastTo(TYPE_DOUBLE).ValueOrDie());

  // double to string
  EXPECT_EQ(Value::String("0"),
            Value::Double(0.0).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("1"),
            Value::Double(1.0).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("1e-07"),
            Value::Double(1e-7).CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("-1.2e+34"),
            Value::Double(-1.2e34).CastTo(TYPE_STRING).ValueOrDie());

  // string to bool
  EXPECT_EQ(Value::Bool(false),
            Value::String("").CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::String("hi").CastTo(TYPE_BOOL).ValueOrDie());
  EXPECT_EQ(Value::Bool(true),
            Value::String(" ").CastTo(TYPE_BOOL).ValueOrDie());

  // string to int64
  EXPECT_TRUE(::util::IsInvalidArgument(
      Value::String("").CastTo(TYPE_INT64).status()));

  // string to double
  EXPECT_TRUE(::util::IsInvalidArgument(
      Value::String("").CastTo(TYPE_DOUBLE).status()));

  // string to string
  EXPECT_EQ(Value::String(""),
            Value::String("").CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String("hi"),
            Value::String("hi").CastTo(TYPE_STRING).ValueOrDie());
  EXPECT_EQ(Value::String(" "),
            Value::String(" ").CastTo(TYPE_STRING).ValueOrDie());
}

}  // namespace
}  // namespace sfdb
