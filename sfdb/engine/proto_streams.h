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
#ifndef SFDB_ENGINE_PROTO_STREAMS_H_
#define SFDB_ENGINE_PROTO_STREAMS_H_

#include <functional>
#include <memory>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/base/db.h"
#include "sfdb/base/proto_stream.h"
#include "util/task/statusor.h"

namespace sfdb {

// A ProtoStream scanning a Table in storage order.
class TableProtoStream : public ProtoStream {
 public:
  explicit TableProtoStream(const Table *t);
  TableProtoStream &operator++() override;
  int GetIndexInTable() const override;
 protected:
  explicit TableProtoStream(const ::google::protobuf::Descriptor *type);
  const std::vector<std::unique_ptr<::google::protobuf::Message>> *rows_;
  size_t i_;
};

// A TableProtoStream over a temporary, non-empty table owned by this object.
class TmpTableProtoStream : public TableProtoStream {
 public:
  explicit TmpTableProtoStream(
      std::vector<std::unique_ptr<::google::protobuf::Message>> &&rows);
 private:
  const std::vector<std::unique_ptr<::google::protobuf::Message>> owned_rows_;
};

// A ProtoStream that filters another stream using a given predicate.
class FilterProtoStream : public ProtoStream {
 public:
  FilterProtoStream() = delete;
  FilterProtoStream(
      std::unique_ptr<ProtoStream> &&src,
      std::function<::util::StatusOr<bool>(const ::google::protobuf::Message&)> pred);
  FilterProtoStream &operator++() override;
 private:
  std::unique_ptr<ProtoStream> src_;
  std::function<::util::StatusOr<bool>(const ::google::protobuf::Message&)> pred_;
};

// A ProtoStream that converts protos from one type to another.
// Classic Google!
class MapProtoStream : public ProtoStream {
 public:
  // A function that takes a proto and returns a new proto.
  using F = std::function<::util::StatusOr<std::unique_ptr<::google::protobuf::Message>>(
      const ::google::protobuf::Message&)>;

  MapProtoStream() = delete;
  MapProtoStream(std::unique_ptr<ProtoStream> &&src,
                 const ::google::protobuf::Descriptor *out_type, F f);
  MapProtoStream &operator++() override;
 private:
  void Apply();
  std::unique_ptr<ProtoStream> src_;
  F f_;
  std::unique_ptr<::google::protobuf::Message> buf_;
};

// A ProtoStream that scans a table using a TableIndex.
class TableIndexProtoStream : public ProtoStream {
 public:
  struct Bound {
    const ::google::protobuf::Message *const msg;
    const bool inclusive;
  };
  TableIndexProtoStream(const TableIndex &index, Bound begin, Bound end);
  TableIndexProtoStream &operator++() override;
  int GetIndexInTable() const override;
 private:
  const TableIndex &index_;
  const decltype(TableIndex::tree.end()) end_;
  decltype(TableIndex::tree.end()) i_;
};

}  // namespace sfdb

#endif  // SFDB_ENGINE_PROTO_STREAMS_H_
