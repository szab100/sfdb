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
#ifndef SFDB_BASE_VARS_H_
#define SFDB_BASE_VARS_H_

#include <memory>
#include <vector>

#include "absl/strings/string_view.h"
#include "google/protobuf/message.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/funcs.h"
#include "sfdb/base/value.h"
#include "util/task/statusor.h"

namespace sfdb {

class MapOverlayVars;
class ProtoOverlayVars;
class DescriptorOverlayVars;

// SQL-accessible variables, constants, and functions.
//
// Each Vars object represents a single scope and may point to a parent Vars
// object. Children may shadow parents' names.
//
// Not thread-safe.
class Vars {
 public:
  explicit Vars(const Vars *parent = nullptr);
  virtual ~Vars() = default;
  virtual ::util::StatusOr<Value> GetVar(::absl::string_view var) const;
  virtual ::util::StatusOr<AstType> GetVarType(
      ::absl::string_view var) const;
  virtual const Func *GetFunc(::absl::string_view func) const;

  // Creates a Vars instance that overlays this one.
  std::unique_ptr<MapOverlayVars> Branch() const;

  // Creats a Vars instance that overlays the parent of this one's.
  std::unique_ptr<MapOverlayVars> MakeSibling() const;

  // The proto must outlive the returned Vars.
  std::unique_ptr<ProtoOverlayVars> Branch(const ::google::protobuf::Message *msg) const;

  // Creates a fake overlay that knows about variable types, but doesn't store
  // any actual values. Useful for AST result type inference.
  std::unique_ptr<DescriptorOverlayVars> Branch(
      const ::google::protobuf::Descriptor *d) const;

 private:
  Vars(const Vars &v) = delete;
  Vars(Vars &&v) = delete;
  Vars &operator=(const Vars &v) = delete;
  Vars &operator=(Vars &&v) = delete;

  const Vars *parent_;
};

// DB-independent built-in constants and functions.
class BuiltIns : public Vars {
 public:
  BuiltIns();
  ::util::StatusOr<Value> GetVar(::absl::string_view var) const override;
  const Func *GetFunc(::absl::string_view fcn) const override;
 private:
  const std::map<std::string, Value> consts_;
  const std::map<std::string, std::unique_ptr<Func>> funcs_;
};

// Overlays parent Vars with a map that can grow.
class MapOverlayVars : public Vars {
 public:
  explicit MapOverlayVars(const Vars *parent);
  void SetVar(::absl::string_view var, const Value &&value);
  ::util::StatusOr<Value> GetVar(::absl::string_view var) const override;
 private:
  std::map<std::string, Value> vars_;
};

// Overlays parent Vars with a read-only proto.
class ProtoOverlayVars : public Vars {
 public:
  ProtoOverlayVars(const Vars *parent, const ::google::protobuf::Message *msg);
  ::util::StatusOr<Value> GetVar(::absl::string_view var) const override;

 private:
  const ::google::protobuf::Message *const msg_;
};

// Overlays parent Vars with a protobuf type that understands variable types,
// but does not store any actual values.
class DescriptorOverlayVars : public Vars {
 public:
  DescriptorOverlayVars(const Vars *parent, const ::google::protobuf::Descriptor *d);
  ::util::StatusOr<AstType> GetVarType(
      ::absl::string_view var) const override;

 private:
  const ::google::protobuf::Descriptor *const d_;
};

}  // namespace sfdb

#endif  // SFDB_BASE_VARS_H_
