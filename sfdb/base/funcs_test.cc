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
#include "sfdb/base/funcs.h"

#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/value.h"
#include "util/task/canonical_errors.h"
#include "util/task/status.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::google::protobuf::FieldDescriptor;
using ::util::IsInvalidArgument;

TEST(FuncsTest, Len) {
  LenFunc len;
  EXPECT_EQ(Value::Int64(0), len({Value::String("")}).ValueOrDie());
  EXPECT_EQ(Value::Int64(1), len({Value::String("x")}).ValueOrDie());
  EXPECT_EQ(Value::Int64(3), len({Value::String("abc")}).ValueOrDie());
  EXPECT_TRUE(IsInvalidArgument(len({}).status()));
  EXPECT_TRUE(IsInvalidArgument(
      len({Value::String("a"), Value::String("b")}).status()));
  EXPECT_EQ(Value::Int64(2), len({Value::Int64(13)}).ValueOrDie());  // bad?

  // type inference
  const AstType str_type =
      AstType::Scalar(FieldDescriptor::TYPE_STRING);
  const AstType t = len.InferReturnType({&str_type}).ValueOrDie();
  EXPECT_FALSE(t.is_void);
  EXPECT_FALSE(t.is_repeated);
  EXPECT_EQ(FieldDescriptor::TYPE_INT64, t.type);
}

TEST(FuncsTest, LowerUpper) {
  LowerFunc lower;
  UpperFunc upper;
  EXPECT_EQ(Value::String("hi there"),
            lower({Value::String("Hi There")}).ValueOrDie());
  EXPECT_EQ(Value::String("HI THERE"),
            upper({Value::String("Hi There")}).ValueOrDie());

  // type inference
  const AstType str_type =
      AstType::Scalar(FieldDescriptor::TYPE_STRING);
  {
    const AstType t = lower.InferReturnType({&str_type}).ValueOrDie();
    EXPECT_EQ(FieldDescriptor::TYPE_STRING, t.type);
  }
  {
    const AstType t = upper.InferReturnType({&str_type}).ValueOrDie();
    EXPECT_EQ(FieldDescriptor::TYPE_STRING, t.type);
  }
}

TEST(FuncsTest, MakeBuiltInFuncs) {
  std::map<std::string, std::unique_ptr<Func>> m = MakeBuiltInFuncs();
  EXPECT_EQ("LEN", m["LEN"]->name);
}

}  // namespace
}  // namespace sfdb
