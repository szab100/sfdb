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
#include "sfdb/base/funcs.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/ascii.h"

namespace sfdb {

using ::util::StatusOr;

std::map<std::string, std::unique_ptr<Func>> MakeBuiltInFuncs() {
  std::map<std::string, std::unique_ptr<Func>> m;

  auto Add = [&m](Func *f) { m[f->name] = std::unique_ptr<Func>(f); };
  Add(new LenFunc);
  Add(new LowerFunc);
  Add(new UpperFunc);

  return m;
}

StatusOr<Value> LenFunc::Exec(const std::string &v) const {
  return Value::Int64(v.size());
}

StatusOr<Value> LowerFunc::Exec(const std::string &v) const {
  return Value::String(::absl::AsciiStrToLower(v));
}

StatusOr<Value> UpperFunc::Exec(const std::string &v) const {
  return Value::String(::absl::AsciiStrToUpper(v));
}

}  // namespace sfdb
