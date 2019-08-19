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
#ifndef SFDB_SQL_PARSER_H_
#define SFDB_SQL_PARSER_H_

#include <memory>

#include "absl/strings/string_view.h"
#include "sfdb/base/ast.h"
#include "util/task/statusor.h"

namespace sfdb {

// Parses a SQL program using recursive descent.
::util::StatusOr<std::unique_ptr<Ast>> Parse(::absl::string_view sql);

}  // namespace sfdb

#endif  // SFDB_SQL_PARSER_H_
