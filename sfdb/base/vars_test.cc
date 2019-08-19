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
#include "sfdb/base/vars.h"

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/value.h"
#include "sfdb/testing/data.pb.h"
#include "util/proto/parse_text_proto.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::util::IsNotFound;

TEST(VarsTest, Vars) {
  BuiltIns vars;
  EXPECT_EQ(Value::Bool(false), vars.GetVar("FALSE").ValueOrDie());
  EXPECT_EQ(Value::Bool(true), vars.GetVar("TRUE").ValueOrDie());

  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_BOOL),
            vars.GetVarType("FALSE").ValueOrDie());
}

TEST(VarsTest, Funcs) {
  BuiltIns vars;
  EXPECT_EQ(
      Value::Int64(3),
      (*vars.GetFunc("LEN"))({Value::String("foo")}).ValueOrDie());
  EXPECT_EQ(
      Value::String("FOO"),
      (*vars.GetFunc("UPPER"))({Value::String("foo")}).ValueOrDie());
}

TEST(VarsTest, MapOverlayVars) {
  BuiltIns root;
  std::unique_ptr<MapOverlayVars> vars = root.Branch();
  EXPECT_EQ(Value::Bool(true), vars->GetVar("true").ValueOrDie());
  EXPECT_FALSE(vars->GetVar("foo").ok());
  vars->SetVar("foo", std::move(Value::String("bar")));
  EXPECT_EQ(Value::String("bar"), vars->GetVar("foo").ValueOrDie());
  EXPECT_TRUE(vars->GetFunc("LEN"));

  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_STRING),
            vars->GetVarType("foo").ValueOrDie());
}

TEST(VarsTest, DoubleBranchAndMakeSibling) {
  BuiltIns root;
  std::unique_ptr<MapOverlayVars> a = root.Branch();
  a->SetVar("a", std::move(Value::String("aa")));
  EXPECT_EQ(Value::String("aa"), a->GetVar("a").ValueOrDie());
  std::unique_ptr<MapOverlayVars> b = a->Branch();
  b->SetVar("b", std::move(Value::String("bb")));
  EXPECT_EQ(Value::String("aa"), b->GetVar("a").ValueOrDie());
  EXPECT_EQ(Value::String("bb"), b->GetVar("b").ValueOrDie());
  std::unique_ptr<MapOverlayVars> c = b->MakeSibling();
  EXPECT_EQ(Value::String("aa"), c->GetVar("a").ValueOrDie());
  EXPECT_TRUE(IsNotFound(c->GetVar("b").status()));
}

TEST(VarsTest, ProtoOverlayVars) {
  const Data msg = PARSE_TEST_PROTO(R"(
    pts: { x: 10 y: 20 weight: 1.0 }
    pts: { x: 13 y: 17 weight: -1.2 }
    pts: { x: 66 y: 99 weight: 1e-7 }
    plot_title: "Howdy")");

  BuiltIns root;
  std::unique_ptr<ProtoOverlayVars> vars = root.Branch(&msg);
  EXPECT_EQ(Value::Bool(true), vars->GetVar("true").ValueOrDie());
  EXPECT_EQ(Value::Int64(10), vars->GetVar("pts[0].x").ValueOrDie());
  EXPECT_EQ(Value::Double(1e-7), vars->GetVar("pts[-1].weight").ValueOrDie());
  EXPECT_EQ(Value::String("Howdy"), vars->GetVar("plot_title").ValueOrDie());
  EXPECT_EQ(Value::Message(std::unique_ptr<Message>(new Data(msg))),
            vars->GetVar("*").ValueOrDie());
  EXPECT_FALSE(vars->GetVar("pts").ok());
  EXPECT_FALSE(vars->GetVar("").ok());

  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_BOOL),
            vars->GetVarType("true").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_INT64),
            vars->GetVarType("pts[0].x").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_DOUBLE),
            vars->GetVarType("pts[-1].weight").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_STRING),
            vars->GetVarType("plot_title").ValueOrDie());
  EXPECT_EQ(AstType::Message(Point::default_instance().GetDescriptor()),
            vars->GetVarType("pts[0]").ValueOrDie());
  EXPECT_EQ(AstType::Message(Data::default_instance().GetDescriptor()),
            vars->GetVarType("*").ValueOrDie());

  /* TODO: allow variables of repeated type?
  EXPECT_EQ(
      AstType::RepeatedMessage(Point::default_instance().GetDescriptor()),
      vars->GetVarType("pts").ValueOrDie());
  */
}

TEST(VarsTest, DescriptorOverlayVars) {
  BuiltIns root;
  std::unique_ptr<DescriptorOverlayVars> vars =
      root.Branch(Data::default_instance().GetDescriptor());
  EXPECT_EQ(Value::Bool(true), vars->GetVar("true").ValueOrDie());
  EXPECT_FALSE(vars->GetVar("pts[0].x").ok());
  EXPECT_FALSE(vars->GetVar("pts[-1].weight").ok());
  EXPECT_FALSE(vars->GetVar("plot_title").ok());
  EXPECT_FALSE(vars->GetVar("pts[0]").ok());
  EXPECT_FALSE(vars->GetVar("*").ok());

  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_BOOL),
            vars->GetVarType("true").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_INT32),
            vars->GetVarType("pts[0].x").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_DOUBLE),
            vars->GetVarType("pts[-1].weight").ValueOrDie());
  EXPECT_EQ(AstType::Scalar(FieldDescriptor::TYPE_STRING),
            vars->GetVarType("plot_title").ValueOrDie());
  EXPECT_EQ(AstType::Message(Point::default_instance().GetDescriptor()),
            vars->GetVarType("pts[0]").ValueOrDie());
  EXPECT_EQ(AstType::Message(Data::default_instance().GetDescriptor()),
            vars->GetVarType("*").ValueOrDie());
}

}  // namespace
}  // namespace sfdb
