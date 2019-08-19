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

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "sfdb/proto/field_path.h"
#include "util/task/canonical_errors.h"

namespace sfdb {

using ::absl::AsciiStrToUpper;
using ::absl::string_view;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::util::IsNotFound;
using ::util::NotFoundError;
using ::util::StatusOr;
using ::util::UnimplementedError;

Vars::Vars(const Vars *parent) : parent_(parent) {
}

StatusOr<Value> Vars::GetVar(string_view var) const {
  if (parent_) return parent_->GetVar(var);
  return NotFoundError(StrCat("No variable called ", var));
}

StatusOr<AstType> Vars::GetVarType(string_view var) const {
  StatusOr<Value> v = GetVar(var);
  if (!v.ok()) return v.status();
  return v.ValueOrDie().type;
}

const Func *Vars::GetFunc(string_view fcn) const {
  if (parent_) return parent_->GetFunc(fcn);
  return nullptr;
}

std::unique_ptr<MapOverlayVars> Vars::Branch() const {
  return std::unique_ptr<MapOverlayVars>(new MapOverlayVars(this));
}

std::unique_ptr<MapOverlayVars> Vars::MakeSibling() const {
  return std::unique_ptr<MapOverlayVars>(new MapOverlayVars(parent_));
}

std::unique_ptr<ProtoOverlayVars> Vars::Branch(const Message *msg) const {
  return std::unique_ptr<ProtoOverlayVars>(new ProtoOverlayVars(this, msg));
}

std::unique_ptr<DescriptorOverlayVars> Vars::Branch(const Descriptor *d) const {
  return std::unique_ptr<DescriptorOverlayVars>(
      new DescriptorOverlayVars(this, d));
}

StatusOr<Value> BuiltIns::GetVar(string_view var) const {
  auto i = consts_.find(AsciiStrToUpper(var));
  if (i != consts_.end()) return i->second;
  return Vars::GetVar(var);
}

const Func *BuiltIns::GetFunc(string_view fcn) const {
  auto i = funcs_.find(AsciiStrToUpper(fcn));
  if (i != funcs_.end()) return i->second.get();
  return Vars::GetFunc(fcn);
}

BuiltIns::BuiltIns() :
    Vars(),
    consts_{
      {"FALSE", Value::Bool(false)},
      {"TRUE", Value::Bool(true)},
    },
    funcs_(MakeBuiltInFuncs()) {}

MapOverlayVars::MapOverlayVars(const Vars *parent) : Vars(parent) {
}

StatusOr<Value> MapOverlayVars::GetVar(string_view var) const {
  auto i = vars_.find(AsciiStrToUpper(var));
  if (i != vars_.end()) return i->second;
  return Vars::GetVar(var);
}

void MapOverlayVars::SetVar(string_view var, const Value &&value) {
  const std::string key = AsciiStrToUpper(var);
  vars_.erase(key);
  vars_.insert({key, std::move(value)});
}

ProtoOverlayVars::ProtoOverlayVars(const Vars *parent, const Message *msg) :
    Vars(parent), msg_(msg) {
}

StatusOr<Value> ProtoOverlayVars::GetVar(string_view var) const {
  // Treat "*" in a special way.
  if (var.empty()) return Vars::GetVar(var);
  if (var == "*") var = "";

  StatusOr<ProtoFieldPath> so =
      ProtoFieldPath::Make(msg_->GetDescriptor(), var);
  if (!so.ok()) return Vars::GetVar(var);
  return so.ValueOrDie().GetFrom(*msg_);
}

DescriptorOverlayVars::DescriptorOverlayVars(
    const Vars *parent, const Descriptor *d) : Vars(parent), d_(d) {
}

StatusOr<AstType> DescriptorOverlayVars::GetVarType(
    string_view var) const {
  // Treat "*" in a special way.
  if (var.empty()) return Vars::GetVarType(var);
  if (var == "*") var = "";

  StatusOr<ProtoFieldPath> so = ProtoFieldPath::Make(d_, var);
  if (IsNotFound(so.status())) return Vars::GetVarType(var);
  if (!so.ok()) return so.status();

  const ProtoFieldPath &pfp = so.ValueOrDie();
  if (pfp.type() == FieldDescriptor::TYPE_GROUP)
    return UnimplementedError("Proto groups are not supported");
  if (pfp.type() == FieldDescriptor::TYPE_MESSAGE) {
    if (!pfp.is_repeated()) return AstType::Message(pfp.message_type());
    return AstType::RepeatedMessage(pfp.message_type());
  }
  if (pfp.type() == FieldDescriptor::TYPE_ENUM) {
    if (!pfp.is_repeated()) return AstType::Enum(pfp.enum_type());
    return AstType::RepeatedEnum(pfp.enum_type());
  }
  if (!pfp.is_repeated()) return AstType::Scalar(pfp.type());
  return AstType::RepeatedScalar(pfp.type());
}

}  // namespace sfdb
