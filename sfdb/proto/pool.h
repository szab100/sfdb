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
#ifndef SFDB_PROTO_POOL_H_
#define SFDB_PROTO_POOL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor_database.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "util/task/statusor.h"

namespace sfdb {

// Wraps a proto2::DescriptorPool that allows the creation of new message types
// at runtime.
class ProtoPool {
 public:
  ProtoPool();

  virtual ~ProtoPool() = default;
  ProtoPool(const ProtoPool &p) = delete;
  ProtoPool(const ProtoPool &&p) = delete;
  ProtoPool &operator=(const ProtoPool &p) = delete;
  ProtoPool &operator=(const ProtoPool &&p) = delete;

  // Creates a new protobuf type with the given name and top-level fields.
  // Each field has a name and a type.
  ::util::StatusOr<const ::google::protobuf::Descriptor*> CreateProtoClass(
      ::absl::string_view name,
      const std::vector<std::pair<std::string, ::google::protobuf::FieldDescriptor::Type>>
          &fields);

  ::util::StatusOr<const ::google::protobuf::Descriptor*> CreateProtoClass(
      ::absl::string_view name,
      const std::vector<::google::protobuf::FieldDescriptorProto> &fields);

  ::util::StatusOr<const ::google::protobuf::Descriptor*> CreateProtoClass(
      std::unique_ptr<::google::protobuf::FileDescriptorProto> &&fdp);

  // Finds any known type given its full name.
  const ::google::protobuf::Descriptor *FindMessageTypeByName(
      ::absl::string_view name) const;

  // Find any known enum type given its full name.
  const ::google::protobuf::EnumDescriptor *FindEnumTypeByName(
      ::absl::string_view name) const;

  // Finds a type that was created with CreateProtoClass(name, ...).
  const ::google::protobuf::Descriptor *FindProtoClass(::absl::string_view name) const;
  // Finds a proto file descriptor that was created with CreateProtoClass(name, ...).
  const ::google::protobuf::FileDescriptor *FindProtoFile(::absl::string_view name) const;

  // Creates an empty proto given its descriptor.
  std::unique_ptr<::google::protobuf::Message> NewMessage(
      const ::google::protobuf::Descriptor *d) const;

  // Creates a proto given its descriptor and a textpb body.
  // Crashes on parse errors. Mainly useful for testing.
  std::unique_ptr<::google::protobuf::Message> NewMessage(
      const ::google::protobuf::Descriptor *d, ::absl::string_view text) const;

  // Creates a branch of this pool that overlays it and can be deleted.
  std::unique_ptr<ProtoPool> Branch() const;

  // Returns a new pool with the same parent as this one.
  std::unique_ptr<ProtoPool> MakeSibling() const;

  ::google::protobuf::MessageFactory *message_factory() { return msg_factory_.get(); }

  // A helper for callers of CreateProtoClass().
  static ::google::protobuf::FieldDescriptorProto::Type TypeToType(
      ::google::protobuf::FieldDescriptor::Type type);

 private:
  ProtoPool(const ::google::protobuf::DescriptorPool &sub_pool);

  const ::google::protobuf::DescriptorPool &sub_pool_;
  std::unique_ptr<::google::protobuf::DescriptorPoolDatabase> sub_pool_db_;
  std::unique_ptr<::google::protobuf::SimpleDescriptorDatabase> overlay_db_;
  std::unique_ptr<::google::protobuf::MergedDescriptorDatabase> db_;
  std::unique_ptr<::google::protobuf::DescriptorPool> pool_;
  std::unique_ptr<::google::protobuf::DynamicMessageFactory> msg_factory_;
};

}  // namespace sfdb

#endif  // SFDB_PROTO_POOL_H_
