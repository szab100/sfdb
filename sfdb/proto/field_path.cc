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
#include "sfdb/proto/field_path.h"

#include <vector>
#include <cstdint>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "re2/re2.h"
#include "re2/stringpiece.h"
#include "sfdb/proto/dup_message.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::StrCat;
using ::absl::string_view;
using ::google::protobuf::Descriptor;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::NotFoundError;
using ::util::Status;
using ::util::StatusOr;
using ::util::UnimplementedError;

namespace {

Status PathError(string_view path, string_view err) {
  return NotFoundError(StrCat(
      "Invalid proto field path (", path, "): ", err));
}

// Converts Python-style array indices into non-negative indices.
StatusOr<int> GetIndexIn(int i, int n) {
  if (i < -n || i >= n) return InvalidArgumentError(StrCat(
      "Trying to get index [", i, "] on a list of size ", n));
  return i + (i < 0) * n;
}

}  // namespace

// static
StatusOr<ProtoFieldPath::Step> ProtoFieldPath::ParseStep(
    const Descriptor *d, string_view word, string_view path) {
  if (word.empty()) return PathError(path, "unexpected dot");

  // Parse the field name.
  std::string field_name;
  static const RE2 kFieldNamePat("(?i)([a-z][a-z0-9_]*)");

  ::re2::StringPiece word_sp(word.data(), word.size());
  ::re2::StringPiece path_sp(path.data(), path.size());

  if (!RE2::Consume(&word_sp, kFieldNamePat, &field_name))
    return PathError(path, StrCat("bad piece ", word));

  // Locate the field's descriptor.
  const FieldDescriptor *fd = d->FindFieldByName(field_name);
  if (!fd) return PathError(path, StrCat("no such field: ", field_name));

  if (word_sp.empty()) {
    // Must not be a repeated field.
    if (fd->is_repeated()) return PathError(path, StrCat(
        field_name, " is a repeated field. Use [] to select an element"));
    return Step{fd};
  }

  // Must be a repeated field.
  int32 repeated_index;
  static const RE2 kRepeatedIndexPat("\\[(-?[0-9]+)\\]");
  if (!RE2::FullMatch(word_sp, kRepeatedIndexPat, &repeated_index))
    return PathError(path, StrCat("unexpected ", word));
  if (!fd->is_repeated())
    return PathError(path, StrCat(field_name, " in not a repeated field"));
  return ProtoFieldPath::Step{fd, repeated_index};
}

// static
StatusOr<ProtoFieldPath> ProtoFieldPath::Make(
    ProtoPool *pool, string_view proto, string_view path) {
  const Descriptor *d = pool->FindMessageTypeByName(std::string(proto));
  if (!d) return NotFoundError(StrCat("Unknown proto type: ", proto));
  return ProtoFieldPath::Make(d, path);
}

// static
StatusOr<ProtoFieldPath> ProtoFieldPath::Make(
    const Descriptor *d, string_view path) {
  const Descriptor *const root = d;
  ::std::vector<Step> steps;
  if (path.empty()) return ProtoFieldPath(root, steps);
  for (string_view word : StrSplit(path, '.')) {
    if (!d) return PathError(path, StrCat(word, " is not a member of parent"));
    StatusOr<Step> r = ParseStep(d, word, path);
    if (!r.ok()) return r.status();
    const Step step = r.ValueOrDie();
    steps.push_back(step);
    d = step.fd->message_type();
  }

  return ProtoFieldPath(root, steps);
}

StatusOr<Value> ProtoFieldPath::GetFrom(const Message &msg) const {
  if (msg.GetDescriptor() != d_) return InvalidArgumentError(StrCat(
      "Applying a ", msg.GetDescriptor()->full_name(), " ProtoFieldPath to",
      " a message of type ", d_->full_name()));

  // Everything but the last step must be a Message.
  const Message *m = &msg;
  for (size_t i = 0; (i + 1) < path_.size(); ++i) {
    const Step &s = path_[i];
    const Reflection *ref = m->GetReflection();
    if (s.fd->type() != FieldDescriptor::TYPE_MESSAGE) return InternalError(
        "Expected a message type along the path");
    if (s.fd->is_repeated()) {
      StatusOr<int> so = GetIndexIn(s.repeated_index, ref->FieldSize(*m, s.fd));
      if (!so.ok()) return so.status();
      m = &ref->GetRepeatedMessage(*m, s.fd, so.ValueOrDie());
    } else {
      m = &ref->GetMessage(*m, s.fd);
    }
  }

  // The last step cannot be a message.
  if (path_.empty()) return Value::Message(DupMessage(&msg));
  const Step &s = path_.back();
  const Reflection *ref = m->GetReflection();
  if (s.fd->is_repeated()) {
    StatusOr<int> so = GetIndexIn(s.repeated_index, ref->FieldSize(*m, s.fd));
    if (!so.ok()) return so.status();
    const int rep_idx = so.ValueOrDie();
    switch (s.fd->type()) {
      case FieldDescriptor::TYPE_BOOL:
        return Value::Bool(ref->GetRepeatedBool(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_INT32:
        return Value::Int64(ref->GetRepeatedInt32(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_INT64:
        return Value::Int64(ref->GetRepeatedInt64(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_FLOAT:
        return Value::Double(ref->GetRepeatedFloat(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_DOUBLE:
        return Value::Double(ref->GetRepeatedDouble(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_STRING:
        return Value::String(ref->GetRepeatedString(*m, s.fd, rep_idx));
      case FieldDescriptor::TYPE_MESSAGE:
        return Value::Message(DupMessage(
            &ref->GetRepeatedMessage(*m, s.fd, rep_idx)));
      default:
        return UnimplementedError("Write more code in field_path.cc");
    }
  } else {
    switch (s.fd->type()) {
      case FieldDescriptor::TYPE_BOOL:
        return Value::Bool(ref->GetBool(*m, s.fd));
      case FieldDescriptor::TYPE_INT32:
        return Value::Int64(ref->GetInt32(*m, s.fd));
      case FieldDescriptor::TYPE_INT64:
        return Value::Int64(ref->GetInt64(*m, s.fd));
      case FieldDescriptor::TYPE_FLOAT:
        return Value::Double(ref->GetFloat(*m, s.fd));
      case FieldDescriptor::TYPE_DOUBLE:
        return Value::Double(ref->GetDouble(*m, s.fd));
      case FieldDescriptor::TYPE_STRING:
        return Value::String(ref->GetString(*m, s.fd));
      case FieldDescriptor::TYPE_MESSAGE:
        return Value::Message(DupMessage(&ref->GetMessage(*m, s.fd)));
      default:
        return UnimplementedError("Write more code in field_path.cc");
    }
  }
}

FieldDescriptor::Type ProtoFieldPath::type() const {
  return path_.empty()
      ? FieldDescriptor::TYPE_MESSAGE
      : path_.back().fd->type();
}

bool ProtoFieldPath::is_repeated() const {
  return false;  // TODO: unimplemented
}

const ::google::protobuf::Descriptor *ProtoFieldPath::message_type() const {
  return path_.empty() ? d_ : path_.back().fd->message_type();
}

const ::google::protobuf::EnumDescriptor *ProtoFieldPath::enum_type() const {
  return path_.empty() ? nullptr : path_.back().fd->enum_type();
}

}  // namespace sfdb
