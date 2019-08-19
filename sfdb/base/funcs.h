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
#ifndef SFDB_BASE_FUNCS_H_
#define SFDB_BASE_FUNCS_H_

#include <map>
#include <memory>
#include <vector>
#include <string>

#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "sfdb/base/ast_type.h"
#include "sfdb/base/value.h"
#include "util/task/canonical_errors.h"
#include "util/task/statusor.h"

namespace sfdb {

// A built-in SQL function.
class Func {
 public:
  const std::string name;

  Func(::absl::string_view name) : name(name) {}
  virtual ~Func() = default;
  virtual ::util::StatusOr<Value> operator()(
      const std::vector<Value> &args) const = 0;
  virtual ::util::StatusOr<AstType> InferReturnType(
      const std::vector<const AstType*> &arg_types) const = 0;
};

// Returns a map from function name to built-in function object.
std::map<std::string, std::unique_ptr<Func>> MakeBuiltInFuncs();

class Func0 : public Func {
 public:
  Func0(::absl::string_view name) : Func(name) {}

  ::util::StatusOr<Value> operator()(
      const std::vector<Value> &args) const override {
    if (!args.empty()) return ::util::InvalidArgumentError(::absl::StrCat(
        name, " called with ", args.size(), " arguments instead of 0"));
    return Exec();
  }

  ::util::StatusOr<AstType> InferReturnType(
      const std::vector<const AstType*> &arg_types) const override {
    if (!arg_types.empty()) return ::util::InvalidArgumentError(::absl::StrCat(
        name, " called with ", arg_types.size(), " arguments instead of 0"));
    return InferReturnType0();
  }

 protected:
  virtual ::util::StatusOr<Value> Exec() const = 0;
  virtual ::util::StatusOr<AstType> InferReturnType0() const = 0;
};

template<::google::protobuf::FieldDescriptor::Type arg0_type>
class Func1 : public Func {
 public:
  Func1(::absl::string_view name) : Func(name) {}

  ::util::StatusOr<Value> operator()(
      const std::vector<Value> &args) const override {
    if (args.size() != 1) return ::util::InvalidArgumentError(::absl::StrCat(
        name, " called with ", args.size(), " arguments instead of 1"));
    if (args[0].type.is_void || args[0].type.is_repeated)
      return ::util::InvalidArgumentError(::absl::StrCat(
          name, " called with an argument of type ", args[0].type.ToString()));
    if (args[0].type.type == arg0_type) return Exec(args[0]);
    ::util::StatusOr<Value> so = args[0].CastTo(arg0_type);
    if (!so.ok()) return so.status();
    return Exec(so.ValueOrDie());
  }

  ::util::StatusOr<AstType> InferReturnType(
      const std::vector<const AstType*> &arg_types) const override {
    if (arg_types.size() != 1)
      return ::util::InvalidArgumentError(::absl::StrCat(
          name, " called with ", arg_types.size(), " arguments instead of 1"));
    return InferReturnType0();
  }

 protected:
  virtual ::util::StatusOr<Value> Exec(const Value &v) const = 0;
  virtual ::util::StatusOr<AstType> InferReturnType0() const = 0;
};

class Func1Str : public Func1<::google::protobuf::FieldDescriptor::TYPE_STRING> {
 public:
  Func1Str(::absl::string_view name) : Func1(name) {}

 protected:
  ::util::StatusOr<Value> Exec(const Value &v) const override {
    return Exec(v.str);
  }
  virtual ::util::StatusOr<Value> Exec(const std::string &v) const = 0;
};

class LenFunc : public Func1Str {
 public:
  LenFunc() : Func1Str("LEN") {}
 protected:
  ::util::StatusOr<Value> Exec(const std::string &v) const override;
  ::util::StatusOr<AstType> InferReturnType0() const override {
    return AstType::Scalar(::google::protobuf::FieldDescriptor::TYPE_INT64);
  }
};

class LowerFunc : public Func1Str {
 public:
  LowerFunc() : Func1Str("LOWER") {}
 protected:
  ::util::StatusOr<Value> Exec(const std::string &v) const override;
  ::util::StatusOr<AstType> InferReturnType0() const override {
    return AstType::Scalar(::google::protobuf::FieldDescriptor::TYPE_STRING);
  }
};

class UpperFunc : public Func1Str {
 public:
  UpperFunc() : Func1Str("UPPER") {}
 protected:
  ::util::StatusOr<Value> Exec(const std::string &v) const override;
  ::util::StatusOr<AstType> InferReturnType0() const override {
    return AstType::Scalar(::google::protobuf::FieldDescriptor::TYPE_STRING);
  }
};

}  // namespace sfdb

#endif  // SFDB_BASE_FUNCS_H_
