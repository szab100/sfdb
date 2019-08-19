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
#include "sfdb/engine/proto_streams.h"

#include <functional>
#include <memory>
#include <vector>

#include "glog/logging.h"

namespace sfdb {

using ::google::protobuf::Descriptor;
using ::google::protobuf::Message;
using ::util::OkStatus;
using ::util::StatusOr;

TableProtoStream::TableProtoStream(const Table *t)
    : ProtoStream(t->type), rows_(&t->rows), i_(-1) {
  ++*this;
}

TableProtoStream::TableProtoStream(const Descriptor *type)
    : ProtoStream(type), rows_(nullptr), i_(-1) {
  // The object is invalid at this point and needs further initialization.
}

int TableProtoStream::GetIndexInTable() const {
  return i_;
}

TableProtoStream &TableProtoStream::operator++() {
  ++i_;
  next_ = (i_ < rows_->size()) ? (*rows_)[i_].get() : nullptr;
  return *this;
}

TmpTableProtoStream::TmpTableProtoStream(
    std::vector<std::unique_ptr<::google::protobuf::Message>> &&rows)
    : TableProtoStream(rows[0]->GetDescriptor()), owned_rows_(std::move(rows)) {
  rows_ = &owned_rows_;
  ++*this;
}

FilterProtoStream::FilterProtoStream(
    std::unique_ptr<ProtoStream> &&src,
    std::function<StatusOr<bool>(const Message&)> pred)
    : ProtoStream(src->type()), src_(std::move(src)), pred_(pred) {
  ++*this;
}

FilterProtoStream &FilterProtoStream::operator++() {
  if (!ok()) return *this;
  if (!src_->ok()) {
    status_ = src_->status();
    return *this;
  }
  next_ = nullptr;
  while (!src_->Done() && !next_) {
    if (!src_->ok()) {
      status_ = src_->status();
      break;
    }
    StatusOr<bool> so = pred_(**src_);
    if (!so.ok()) {
      status_ = so.status();
      break;
    }
    if (so.ValueOrDie()) next_ = src_->operator->();
    ++*src_;
  }
  return *this;
}

MapProtoStream::MapProtoStream(
    std::unique_ptr<ProtoStream> &&src, const Descriptor *out_type,
    MapProtoStream::F f)
    : ProtoStream(out_type), src_(std::move(src)), f_(f) {
  Apply();
}

MapProtoStream &MapProtoStream::operator++() {
  if (ok()) {
    ++*src_;
    Apply();
  }
  return *this;
}

void MapProtoStream::Apply() {
  buf_ = nullptr;
  next_ = nullptr;
  if (src_->Done()) return;
  StatusOr<std::unique_ptr<Message>> so = f_(**src_);
  if (!so.ok()) {
    status_ = so.status();
  } else {
    buf_ = std::move(so.ValueOrDie());
    next_ = buf_.get();
  }
}

TableIndexProtoStream::TableIndexProtoStream(
    const TableIndex &index, Bound begin, Bound end) :
    ProtoStream(index.t->type), index_(index),
    end_(end.inclusive ?
         index.tree.upper_bound({end.msg, index.t->rows.size()}) :
         index.tree.lower_bound({end.msg, -1})),
    i_(begin.inclusive ?
       index.tree.lower_bound({begin.msg, -1}) :
       index.tree.upper_bound({begin.msg, index.t->rows.size()})) {
  if (i_ != index_.tree.end() && end_ != index_.tree.end()) {
    if (!index_.tree.key_comp()(*i_, *end_)) i_ = index_.tree.end();
  }
  if (i_ != index_.tree.end()) next_ = i_->first;
}

TableIndexProtoStream &TableIndexProtoStream::operator++() {
  CHECK(i_ != index_.tree.end());
  if (++i_ != end_) {
    next_ = i_->first;
  } else {
    i_ = index_.tree.end();
    next_ = nullptr;
  }
  return *this;
}

int TableIndexProtoStream::GetIndexInTable() const {
  CHECK(i_ != index_.tree.end());
  return i_->second;
}

}  // namespace sfdb
