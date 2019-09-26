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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::string_view;
using ::absl::StrCat;
using ::google::protobuf::Descriptor;
using ::google::protobuf::DescriptorPool;
using ::google::protobuf::DescriptorPoolDatabase;
using ::google::protobuf::DescriptorProto;
using ::google::protobuf::DynamicMessageFactory;
using ::google::protobuf::EnumDescriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FieldDescriptorProto;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorProto;
using ::google::protobuf::MergedDescriptorDatabase;
using ::google::protobuf::Message;
using ::google::protobuf::SimpleDescriptorDatabase;
using ::google::protobuf::TextFormat;
using ::util::InternalError;
using ::util::InvalidArgumentError;
using ::util::StatusOr;

std::string GenProtoFileName(::absl::string_view name) {
  return StrCat("sfdb/runtime/", name, ".proto");
}

// static
FieldDescriptorProto::Type ProtoPool::TypeToType(FieldDescriptor::Type type) {
  switch (type) {
    case FieldDescriptor::TYPE_DOUBLE:
        return FieldDescriptorProto::TYPE_DOUBLE;
    case FieldDescriptor::TYPE_FLOAT:
        return FieldDescriptorProto::TYPE_FLOAT;
    case FieldDescriptor::TYPE_INT64:
        return FieldDescriptorProto::TYPE_INT64;
    case FieldDescriptor::TYPE_UINT64:
        return FieldDescriptorProto::TYPE_UINT64;
    case FieldDescriptor::TYPE_INT32:
        return FieldDescriptorProto::TYPE_INT32;
    case FieldDescriptor::TYPE_FIXED64:
        return FieldDescriptorProto::TYPE_FIXED64;
    case FieldDescriptor::TYPE_FIXED32:
        return FieldDescriptorProto::TYPE_FIXED32;
    case FieldDescriptor::TYPE_BOOL:
        return FieldDescriptorProto::TYPE_BOOL;
    case FieldDescriptor::TYPE_STRING:
        return FieldDescriptorProto::TYPE_STRING;
    case FieldDescriptor::TYPE_GROUP:
        return FieldDescriptorProto::TYPE_GROUP;
    case FieldDescriptor::TYPE_MESSAGE:
        return FieldDescriptorProto::TYPE_MESSAGE;
    case FieldDescriptor::TYPE_BYTES:
        return FieldDescriptorProto::TYPE_BYTES;
    case FieldDescriptor::TYPE_UINT32:
        return FieldDescriptorProto::TYPE_UINT32;
    case FieldDescriptor::TYPE_ENUM:
        return FieldDescriptorProto::TYPE_ENUM;
    case FieldDescriptor::TYPE_SFIXED32:
        return FieldDescriptorProto::TYPE_SFIXED32;
    case FieldDescriptor::TYPE_SFIXED64:
        return FieldDescriptorProto::TYPE_SFIXED64;
    case FieldDescriptor::TYPE_SINT32:
        return FieldDescriptorProto::TYPE_SINT32;
    case FieldDescriptor::TYPE_SINT64:
        return FieldDescriptorProto::TYPE_SINT64;
    default: LOG(FATAL) << "Unkown type " << type
      << " in ProtoPool::TypeToType()";
  }
}

ProtoPool::ProtoPool(const DescriptorPool &sub_pool) :
    sub_pool_(sub_pool),
    sub_pool_db_(new DescriptorPoolDatabase(sub_pool)),
    overlay_db_(new SimpleDescriptorDatabase),
    db_(new MergedDescriptorDatabase(overlay_db_.get(), sub_pool_db_.get())),
    pool_(new DescriptorPool(db_.get())),
    msg_factory_(new DynamicMessageFactory) {
  msg_factory_->SetDelegateToGeneratedFactory(true);
}

ProtoPool::ProtoPool() : ProtoPool(*DescriptorPool::generated_pool()) {}

std::unique_ptr<ProtoPool> ProtoPool::Branch() const {
  return std::unique_ptr<ProtoPool>(new ProtoPool(*pool_));
}

std::unique_ptr<ProtoPool> ProtoPool::MakeSibling() const {
  return std::unique_ptr<ProtoPool>(new ProtoPool(sub_pool_));
}

StatusOr<const Descriptor*> ProtoPool::CreateProtoClass(
    std::unique_ptr<FileDescriptorProto> &&fdp) {
  if (fdp->message_type_size() != 1) return InvalidArgumentError(
      "FileDescriptorProto must contain exactly one ProtoDescriptor");
  const std::string name = fdp->message_type(0).name();

  // Check if descriptor already exists in client descriptor db
  const Descriptor *ed = FindProtoClass(name);
  if (ed) {
    return ed;
  }

  if (!overlay_db_->AddAndOwn(fdp.release()))
    return InternalError("Failed to create proto descriptor");
  const Descriptor *d = FindProtoClass(name);
  if (!d) return InternalError(StrCat("Created proto descriptor for ", name, ", but lost it."));
  return d;
}

StatusOr<const Descriptor*> ProtoPool::CreateProtoClass(
    string_view name, const std::vector<FieldDescriptorProto> &fields) {
  std::unique_ptr<FileDescriptorProto> file(new FileDescriptorProto);
  file->set_name(GenProtoFileName(name));
  file->set_package("sfdb.runtime");
  file->set_syntax("proto2");
  DescriptorProto *message = file->add_message_type();
  message->set_name(name.data(), name.size());
  for (size_t i = 0; i < fields.size(); ++i) *message->add_field() = fields[i];
  return CreateProtoClass(std::move(file));
}

StatusOr<const Descriptor*> ProtoPool::CreateProtoClass(
      string_view name,
      const std::vector<std::pair<std::string, FieldDescriptor::Type>> &fields) {
  std::vector<FieldDescriptorProto> fds(fields.size());
  for (size_t i = 0; i < fields.size(); ++i) {
    fds[i].set_name(fields[i].first);
    fds[i].set_number(i + 1);
    fds[i].set_label(FieldDescriptorProto::LABEL_OPTIONAL);
    fds[i].set_type(TypeToType(fields[i].second));
  }
  return CreateProtoClass(name, fds);
}

const Descriptor *ProtoPool::FindProtoClass(string_view name) const {
  return FindMessageTypeByName(StrCat("sfdb.runtime.", name));
}

const FileDescriptor *ProtoPool::FindProtoFile(string_view name) const {
  return pool_->FindFileByName(GenProtoFileName(name));
}

std::unique_ptr<Message> ProtoPool::NewMessage(const Descriptor *d) const {
  return std::unique_ptr<Message>(msg_factory_->GetPrototype(d)->New());
}

std::unique_ptr<Message> ProtoPool::NewMessage(
    const Descriptor *d, string_view text) const {
  std::unique_ptr<Message> m = NewMessage(d);
  ::google::protobuf::io::ArrayInputStream input_stream(text.data(), text.size());
  CHECK(TextFormat::Parse(&input_stream, m.get()))
      << "Unparseable textpb for a message of type " << d->full_name()
      << ":\n  " << text;
  return m;
}

const Descriptor *ProtoPool::FindMessageTypeByName(string_view name) const {
  return pool_->FindMessageTypeByName(std::string(name));
}

const EnumDescriptor *ProtoPool::FindEnumTypeByName(string_view name) const {
  return pool_->FindEnumTypeByName(std::string(name));
}

}  // namespace sfdb
