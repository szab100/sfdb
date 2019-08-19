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
#ifndef SFDB_ENGINE_SET_FIELD_H_
#define SFDB_ENGINE_SET_FIELD_H_

#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "sfdb/base/value.h"
#include "sfdb/proto/pool.h"
#include "util/task/status.h"

namespace sfdb {

// TODO: move this file to ../proto/

// Sets the field described by |fd| in |msg| to the value |v|.
::util::Status SetField(
    const Value &v, const ::google::protobuf::FieldDescriptor *fd,
    ProtoPool *pool, ::google::protobuf::Message *msg);

}  // namespace sfdb

#endif  // SFDB_ENGINE_SET_FIELD_H_
