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
#include "util/task/canonical_errors.h"

#include "util/task/codes.pb.h"

namespace util {
Status AbortedError(absl::string_view message) {
  return Status(::util::error::ABORTED, message);
}

Status AlreadyExistsError(absl::string_view message) {
  return Status(::util::error::ALREADY_EXISTS, message);
}

Status CancelledError(absl::string_view message) {
  return Status(::util::error::CANCELLED, message);
}

Status DataLossError(absl::string_view message) {
  return Status(::util::error::DATA_LOSS, message);
}

Status DeadlineExceededError(absl::string_view message) {
  return Status(::util::error::DEADLINE_EXCEEDED, message);
}

Status FailedPreconditionError(absl::string_view message) {
  return Status(::util::error::FAILED_PRECONDITION, message);
}

Status InternalError(absl::string_view message) {
  return Status(::util::error::INTERNAL, message);
}

Status InvalidArgumentError(absl::string_view message) {
  return Status(::util::error::INVALID_ARGUMENT, message);
}

Status NotFoundError(absl::string_view message) {
  return Status(::util::error::NOT_FOUND, message);
}

Status OutOfRangeError(absl::string_view message) {
  return Status(::util::error::OUT_OF_RANGE, message);
}

Status PermissionDeniedError(absl::string_view message) {
  return Status(::util::error::PERMISSION_DENIED, message);
}

Status ResourceExhaustedError(absl::string_view message) {
  return Status(::util::error::RESOURCE_EXHAUSTED, message);
}

Status UnauthenticatedError(absl::string_view message) {
  return Status(::util::error::PERMISSION_DENIED, message);
}

Status UnavailableError(absl::string_view message) {
  return Status(::util::error::UNAVAILABLE, message);
}

Status UnimplementedError(absl::string_view message) {
  return Status(::util::error::UNIMPLEMENTED, message);
}

Status UnknownError(absl::string_view message) {
  return Status(::util::error::UNKNOWN, message);
}

bool IsAborted(const Status& status) {
  return status.code() == ::util::error::ABORTED;
}

bool IsAlreadyExists(const Status& status) {
  return status.code() == ::util::error::ALREADY_EXISTS;
}

bool IsCancelled(const Status& status) {
  return status.code() == ::util::error::CANCELLED;
}

bool IsDataLoss(const Status& status) {
  return status.code() == ::util::error::DATA_LOSS;
}

bool IsDeadlineExceeded(const Status& status) {
  return status.code() == ::util::error::DEADLINE_EXCEEDED;
}

bool IsFailedPrecondition(const Status& status) {
  return status.code() == ::util::error::FAILED_PRECONDITION;
}

bool IsInternal(const Status& status) {
  return status.code() == ::util::error::INTERNAL;
}

bool IsInvalidArgument(const Status& status) {
  return status.code() == ::util::error::INVALID_ARGUMENT;
}

bool IsNotFound(const Status& status) {
  return status.code() == ::util::error::NOT_FOUND;
}

bool IsOutOfRange(const Status& status) {
  return status.code() == ::util::error::OUT_OF_RANGE;
}

bool IsPermissionDenied(const Status& status) {
  return status.code() == ::util::error::PERMISSION_DENIED;
}

bool IsResourceExhausted(const Status& status) {
  return status.code() == ::util::error::RESOURCE_EXHAUSTED;
}

bool IsUnauthenticated(const Status& status) {
  return status.code() == ::util::error::PERMISSION_DENIED;
}

bool IsUnavailable(const Status& status) {
  return status.code() == ::util::error::UNAVAILABLE;
}

bool IsUnimplemented(const Status& status) {
  return status.code() == ::util::error::UNIMPLEMENTED;
}

bool IsUnknown(const Status& status) {
  return status.code() == ::util::error::UNKNOWN;
}

}  // namespace util
