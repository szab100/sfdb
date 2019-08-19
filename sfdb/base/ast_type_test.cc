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
#include "sfdb/base/ast_type.h"

#include "google/protobuf/descriptor.h"
#include "gtest/gtest.h"
#include "sfdb/proto/pool.h"
#include "sfdb/testing/data.pb.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::google::protobuf::FieldDescriptor;

TEST(AstTypeTest, FromString) {
  const ProtoPool pool;
  auto Go = [&pool](const char *s) {
    return AstType::FromString(s, pool).ValueOrDie();
  };

  EXPECT_TRUE(Go("void").is_void);
  EXPECT_FALSE(Go("void").is_repeated);

  EXPECT_TRUE(Go("void[]").is_void);
  EXPECT_TRUE(Go("void[]").is_repeated);

  EXPECT_EQ(FieldDescriptor::TYPE_INT32, Go("int32").type);
  EXPECT_FALSE(Go("int32").is_void);
  EXPECT_FALSE(Go("int32").is_repeated);

  EXPECT_EQ(FieldDescriptor::TYPE_DOUBLE, Go("double[]").type);
  EXPECT_FALSE(Go("double[]").is_void);
  EXPECT_TRUE(Go("double[]").is_repeated);

  EXPECT_TRUE(Go("string").IsString());

  const Data d;
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, Go("Proto<sfdb.Data>").type);
  EXPECT_EQ(d.GetDescriptor()->name(), Go("Proto<sfdb.Data>").d->name());
  EXPECT_FALSE(Go("Proto<sfdb.Data>").is_repeated);
  EXPECT_TRUE(Go("Proto<sfdb.Data>").IsMessage());

  const Point p;
  EXPECT_EQ(FieldDescriptor::TYPE_MESSAGE, Go("Proto<sfdb.Point>[]").type);
  EXPECT_EQ(p.GetDescriptor()->name(), Go("Proto<sfdb.Point>[]").d->name());
  EXPECT_TRUE(Go("Proto<sfdb.Point>[]").is_repeated);
}

}  // namespace
}  // namespace sfdb
