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
#include "sfdb/proto/pool.h"

#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "google/protobuf/reflection.h"
#include "gtest/gtest.h"
#include "util/task/statusor.h"

namespace sfdb {
namespace {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;

TEST(PoolTest, Basic) {
  ProtoPool pool;
  const Descriptor *d = pool.CreateProtoClass("Person", {
      {"name", FieldDescriptor::TYPE_STRING},
      {"age", FieldDescriptor::TYPE_INT64}}).ValueOrDie();
  ASSERT_TRUE(d);
  const FieldDescriptor *name_fd = d->FindFieldByName("name");
  ASSERT_TRUE(name_fd);
  const FieldDescriptor *age_fd = d->FindFieldByName("age");
  ASSERT_TRUE(age_fd);

  std::unique_ptr<Message> person = pool.NewMessage(d);
  const Reflection *ref = person->GetReflection();
  ref->SetString(person.get(), name_fd, "Methuzelah");
  ref->SetInt64(person.get(), age_fd, 1000);
  ASSERT_EQ("name: \"Methuzelah\"\nage: 1000\n", person->DebugString());

  std::unique_ptr<Message> person2 = pool.NewMessage(d, "name: 'Bob' age: 13");
  ASSERT_EQ("name: \"Bob\"\nage: 13\n", person2->DebugString());
}

TEST(PoolTest, Branch) {
  ProtoPool parent;
  std::unique_ptr<ProtoPool> child = parent.Branch();
  child->CreateProtoClass(
      "Empty", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  ASSERT_TRUE(child->FindProtoClass("Empty"));
  ASSERT_FALSE(parent.FindProtoClass("Empty"));
}

TEST(PoolTest, DoubleBranch) {
  ProtoPool a;
  std::unique_ptr<ProtoPool> b = a.Branch();
  std::unique_ptr<ProtoPool> c = b->Branch();
  b->CreateProtoClass(
      "Empty", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  ASSERT_FALSE(a.FindProtoClass("Empty"));
  ASSERT_TRUE(b->FindProtoClass("Empty"));
  ASSERT_TRUE(c->FindProtoClass("Empty"));
}

TEST(PoolTest, MakeSibling) {
  ProtoPool a;
  a.CreateProtoClass("A", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  std::unique_ptr<ProtoPool> b = a.MakeSibling();
  b->CreateProtoClass("B", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  ASSERT_TRUE(a.FindProtoClass("A"));
  ASSERT_FALSE(a.FindProtoClass("B"));
  ASSERT_FALSE(b->FindProtoClass("A"));
  ASSERT_TRUE(b->FindProtoClass("B"));
}

TEST(PoolTest, MakeSibling2) {
  ProtoPool a;
  std::unique_ptr<ProtoPool> b = a.Branch();
  b->CreateProtoClass("B", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  std::unique_ptr<ProtoPool> c = b->MakeSibling();
  c->CreateProtoClass("C", std::vector<FieldDescriptorProto>{}).ValueOrDie();
  ASSERT_TRUE(b->FindProtoClass("B"));
  ASSERT_FALSE(b->FindProtoClass("C"));
  ASSERT_FALSE(c->FindProtoClass("B"));
  ASSERT_TRUE(c->FindProtoClass("C"));
}

}  // namespace
}  // namespace sfdb
