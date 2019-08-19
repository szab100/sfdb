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
#include "util/task/status.h"

#include <unordered_map>

#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "glog/logging.h"

using ::std::ostream;
using ::std::string;

namespace util {

namespace {

const std::unordered_map<::util::error::Code, ::grpc::StatusCode>
    kGrpcStatusCodeMap = {
        std::make_pair(::util::error::OK, ::grpc::OK),
        std::make_pair(::util::error::CANCELLED, ::grpc::CANCELLED),
        std::make_pair(::util::error::UNKNOWN, ::grpc::UNKNOWN),
        std::make_pair(::util::error::INVALID_ARGUMENT,
                       ::grpc::INVALID_ARGUMENT),
        std::make_pair(::util::error::DEADLINE_EXCEEDED,
                       ::grpc::DEADLINE_EXCEEDED),
        std::make_pair(::util::error::NOT_FOUND, ::grpc::NOT_FOUND),
        std::make_pair(::util::error::ALREADY_EXISTS, ::grpc::ALREADY_EXISTS),
        std::make_pair(::util::error::PERMISSION_DENIED,
                       ::grpc::PERMISSION_DENIED),
        std::make_pair(::util::error::PERMISSION_DENIED,
                       ::grpc::UNAUTHENTICATED),
        std::make_pair(::util::error::RESOURCE_EXHAUSTED,
                       ::grpc::RESOURCE_EXHAUSTED),
        std::make_pair(::util::error::FAILED_PRECONDITION,
                       ::grpc::FAILED_PRECONDITION),
        std::make_pair(::util::error::ABORTED, ::grpc::ABORTED),
        std::make_pair(::util::error::OUT_OF_RANGE, ::grpc::OUT_OF_RANGE),
        std::make_pair(::util::error::UNIMPLEMENTED, ::grpc::UNIMPLEMENTED),
        std::make_pair(::util::error::INTERNAL, ::grpc::INTERNAL),
        std::make_pair(::util::error::UNAVAILABLE, ::grpc::UNAVAILABLE),
        std::make_pair(::util::error::DATA_LOSS, ::grpc::DATA_LOSS),
};

const Status& GetOk() {
  static const Status status;
  return status;
}

const Status& GetCancelled() {
  static const Status status(::util::error::CANCELLED, "");
  return status;
}

const Status& GetUnknown() {
  static const Status status(::util::error::UNKNOWN, "");
  return status;
}

}  // namespace

Status::Status() : code_(::util::error::OK), message_("") {}

Status::Status(::util::error::Code error, absl::string_view error_message)
    : code_(error), message_(error_message) {
  if (code_ == ::util::error::OK) {
    message_.clear();
  }
}

Status::Status(const Status& other)
    : code_(other.code_), message_(other.message_) {}

Status& Status::operator=(const Status& other) {
  code_ = other.code_;
  message_ = other.message_;
  return *this;
}

const Status& Status::OK = GetOk();
const Status& Status::CANCELLED = GetCancelled();
const Status& Status::UNKNOWN = GetUnknown();

string Status::ToString() const {
  if (code_ == ::util::error::OK) {
    return "OK";
  }

  return ::absl::Substitute("$0: $1", code_, message_);
}

extern ostream& operator<<(ostream& os, const Status& other) {
  os << other.ToString();
  return os;
}

Status::operator ::grpc::Status() const {
  auto it = kGrpcStatusCodeMap.find(this->code_);

  return ::grpc::Status(
      it != kGrpcStatusCodeMap.end() ? it->second : ::grpc::UNKNOWN,
      this->error_message());
}

}  // namespace util
