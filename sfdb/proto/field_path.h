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
#ifndef SFDB_PROTO_FIELD_PATH_H_
#define SFDB_PROTO_FIELD_PATH_H_

#include <vector>

#include "sfdb/base/value.h"
#include "sfdb/proto/pool.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "absl/strings/string_view.h"
#include "util/task/statusor.h"
#include "util/types/integral_types.h"

namespace sfdb {

// A path to a field in a particular protobuf type.
//
// For example, "EventId.server_ip" is a path in GWSLogEntryProto.
//
// Immutable.
class ProtoFieldPath {
 public:
  ProtoFieldPath() = delete;

  static ::util::StatusOr<ProtoFieldPath> Make(
      ProtoPool *pool, ::absl::string_view proto, ::absl::string_view path);

  static ::util::StatusOr<ProtoFieldPath> Make(
      const ::google::protobuf::Descriptor *d, ::absl::string_view path);

  // Returns an error if the path is not a scalar, or invalid in any other way.
  ::util::StatusOr<Value> GetFrom(const ::google::protobuf::Message &msg) const;

  // Analogues of FieldDescriptor methods.
  ::google::protobuf::FieldDescriptor::Type type() const;
  bool is_repeated() const;
  const ::google::protobuf::Descriptor *message_type() const;
  const ::google::protobuf::EnumDescriptor *enum_type() const;

 private:
  struct Step {
    const ::google::protobuf::FieldDescriptor *const fd;
    const int32 repeated_index;  // for repeated fields
  };

  // Parses one piece of the path, which is a protobuf field name,
  // optionally followed by a square-bracketed index (for repeated fields).
  static ::util::StatusOr<Step> ParseStep(
      const ::google::protobuf::Descriptor *d, ::absl::string_view word,
      ::absl::string_view path);

  ProtoFieldPath(const ::google::protobuf::Descriptor *d, const ::std::vector<Step> path)
      : d_(d), path_(path) {}

  const ::google::protobuf::Descriptor *const d_;
  const ::std::vector<Step> path_;
};

}  // namespace sfdb

#endif  // SFDB_PROTO_FIELD_PATH_H_
