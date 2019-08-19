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
#ifndef SFDB_BASE_PROTO_STREAM_H_
#define SFDB_BASE_PROTO_STREAM_H_

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "util/task/status.h"

namespace sfdb {

// A read-only iterator over a list of Message objects.
//
// Not thread-safe.
//
// Example usage:
//   std::unique_ptr<ProtoStream> ps(new SomeProtoStream(...));
//   while (!ps->Done()) {
//     if (!ps->ok()) return ps->status();
//     HandleProto(**ps);
//     ++*ps;
//   }
//   return ::util::OkStatus();
class ProtoStream {
 public:
  virtual ~ProtoStream() = default;

  // The type of protos to expect from this stream.
  const ::google::protobuf::Descriptor *type() const { return type_; }

  // Iterator interface
  ::util::Status status() const { return status_; }
  bool ok() const { return status().ok(); }
  bool Done() const { return !next_; }
  const ::google::protobuf::Message *operator->() const { return next_; }
  const ::google::protobuf::Message &operator*() const { return *next_; }

  // Returns the index of this row in its table.
  // This only makes sense for TableProtoStream and TableIndexProtoStream.
  // For temporary streams (like Map and Filter), crashes.
  virtual int GetIndexInTable() const {
    LOG(FATAL) << "No table in temporary stream";
  }

  virtual ProtoStream &operator++() = 0;

 protected:
  ProtoStream(const ::google::protobuf::Descriptor *type) :
      type_(type), status_(::util::OkStatus()), next_(nullptr) {}

  const ::google::protobuf::Descriptor *const type_;
  ::util::Status status_;
  const ::google::protobuf::Message *next_;
};

}  // namespace sfdb

#endif  // SFDB_BASE_PROTO_STREAM_H_
