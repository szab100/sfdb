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
#include "sfdb/engine/set_field.h"

#include "util/task/canonical_errors.h"

namespace sfdb {

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;
using ::util::InternalError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;
using ::util::UnimplementedError;

Status SetField(
    const Value &v, const FieldDescriptor *fd, ProtoPool *pool,
    Message *msg) {
  if (fd->containing_type() != msg->GetDescriptor())
    return InternalError("Field type is not a member of message type");

  StatusOr<Value> so = v.CastTo(fd->type());
  if (!so.ok()) return so.status();
  const Value &w = so.ValueOrDie();

  const Reflection *r = msg->GetReflection();
  switch (fd->type()) {
    case FieldDescriptor::TYPE_DOUBLE:
      r->SetDouble(msg, fd, w.dbl);
      break;
    case FieldDescriptor::TYPE_FLOAT:
      r->SetFloat(msg, fd, w.dbl);
      break;
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_SFIXED64:
      r->SetInt64(msg, fd, w.i64);
      break;
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED64:
      r->SetUInt64(msg, fd, w.i64);
      break;
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SFIXED32:
      r->SetInt32(msg, fd, w.i64);
      break;
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_UINT32:
      r->SetUInt32(msg, fd, w.i64);
      break;
    case FieldDescriptor::TYPE_BOOL:
      r->SetBool(msg, fd, w.boo);
      break;
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_BYTES:
      r->SetString(msg, fd, w.str);
      break;
    case FieldDescriptor::TYPE_GROUP:
      return UnimplementedError("Groups are not supported");
    case FieldDescriptor::TYPE_MESSAGE:
      r->MutableMessage(msg, fd, pool->message_factory())->CopyFrom(*v.msg);
      break;
    case FieldDescriptor::TYPE_ENUM:
      r->SetEnumValue(msg, fd, w.i64);  // TODO: use SetEnum()?
      break;
  }

  return OkStatus();
}

}  // namespace sfdb
