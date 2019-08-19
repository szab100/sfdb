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
#ifndef UTIL_TASK_STATUSOR_H_
#define UTIL_TASK_STATUSOR_H_

#include <type_traits>

#include "absl/meta/type_traits.h"
#include "glog/logging.h"
#include "util/task/status.h"

namespace util {

template <typename T>
class StatusOr;

template <typename T, typename U>
using IsStatusOrConversionAmbiguous =
    absl::disjunction<std::is_constructible<T, StatusOr<U>&>,
                      std::is_constructible<T, const StatusOr<U>&>,
                      std::is_constructible<T, StatusOr<U>&&>,
                      std::is_constructible<T, const StatusOr<U>&&>,
                      std::is_convertible<StatusOr<U>&, T>,
                      std::is_convertible<const StatusOr<U>&, T>,
                      std::is_convertible<StatusOr<U>&&, T>,
                      std::is_convertible<const StatusOr<U>&&, T>>;

// A StatusOr holds a Status (in the case of an error), or a value T.
template <typename T>
class StatusOr {
 public:
  // Has status UNKNOWN.
  explicit inline StatusOr();

  // Builds from a non-OK status. Crashes if an OK status is specified.
  inline StatusOr(const ::util::Status& status);  // NOLINT

  // Builds from the specified value.
  inline StatusOr(const T& value);  // NOLINT
  inline StatusOr(T&& value);       // NOLINT

  StatusOr(StatusOr&& other)
      : status_(std::move(other.status_)), value_(std::move(other.value_)){};
  StatusOr& operator=(StatusOr&& other) {
    status_ = std::move(other.status_);
    value_ = std::move(other.value_);
    return *this;
  }

  // Builds from the specified value.
  inline StatusOr(const ::util::Status& status, const T& value);  // NOLINT
  inline StatusOr(const ::util::Status& status, T&& value);       // NOLINT

  // Copy constructor.
  template <typename U,
            absl::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<T, U>>,
                    std::is_constructible<T, const U&>,
                    std::is_convertible<const U&, T>,
                    absl::negation<IsStatusOrConversionAmbiguous<T, U>>>::value,
                int> = 0>
  StatusOr(const StatusOr<U>& other)  // NOLINT
      : status_(other.status_), value_(other.value_) {}

  template <typename U,
            absl::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<T, U>>,
                    std::is_constructible<T, const U&>,
                    absl::negation<std::is_convertible<const U&, T>>,
                    absl::negation<IsStatusOrConversionAmbiguous<T, U>>>::value,
                int> = 0>
  explicit StatusOr(const StatusOr<U>& other)
      : status_(other.status_), value_{other.value_} {}

  // Move constructor.
  template <typename U,
            absl::enable_if_t<
                absl::conjunction<
                    absl::negation<std::is_same<T, U>>,
                    std::is_constructible<T, U&&>, std::is_convertible<U&&, T>,
                    absl::negation<IsStatusOrConversionAmbiguous<T, U>>>::value,
                int> = 0>
  inline StatusOr(StatusOr<U>&& other)
      : status_(std::move(other.status_)), value_{std::move(other.value_)} {}

  // Conversion copy constructor, T must be copy constructible from U.
  // template <typename U>
  // inline StatusOr(const StatusOr<U>& other);

  // Assignment operator.
  inline const StatusOr& operator=(const StatusOr& other);

  // Conversion assignment operator, T must be assignable from U
  template <typename U>
  inline const StatusOr& operator=(const StatusOr<U>& other);

  // Accessors.
  inline const ::util::Status& status() const { return status_; }

  // Shorthand for status().ok().
  inline bool ok() const { return status_.ok(); }

  // Returns value or crashes if ok() is false.
  const T& ValueOrDie() const& {
    CHECK(ok()) << "Attempting to fetch value of non-OK StatusOr";
    return this->value_;
  }

  T& ValueOrDie() & {
    CHECK(ok()) << "Attempting to fetch value of non-OK StatusOr";
    return this->value_;
  }

  const T&& ValueOrDie() const&& {
    CHECK(ok()) << "Attempting to fetch value of non-OK StatusOr";
    return std::move(this->value_);
  }

  T&& ValueOrDie() && {
    CHECK(ok()) << "Attempting to fetch value of non-OK StatusOr";
    return std::move(this->value_);
  }

  virtual ~StatusOr() {
    if (status_.ok()) {
      value_.~T();
    }
  }

 private:
  Status status_;
  struct Dummy {};
  union {
    // Dummy is used to avoid initialization of data_ when it has
    // deleted constructor.
    Dummy dummy_;
    T value_;
  };

  template <typename U>
  friend class StatusOr;
};

// Implementation.

template <typename T>
inline StatusOr<T>::StatusOr() : status_(::util::error::UNKNOWN, "") {}

template <typename T>
inline StatusOr<T>::StatusOr(const ::util::Status& status) : status_(status) {
  CHECK(!status.ok()) << "Status::OK is not a valid argument to StatusOr";
}

template <typename T>
inline StatusOr<T>::StatusOr(const T& value) : value_(value) {}

template <typename T>
inline StatusOr<T>::StatusOr(T&& value) : value_(std::move(value)) {}

template <typename T>
inline StatusOr<T>::StatusOr(const ::util::Status& status, const T& value)
    : status_(status), value_(value) {}

template <typename T>
inline StatusOr<T>::StatusOr(const ::util::Status& status, T&& value)
    : status_(status), value_(std::move(value)) {}

template <typename T>
inline const StatusOr<T>& StatusOr<T>::operator=(const StatusOr& other) {
  status_ = other.status_;
  if (status_.ok()) {
    value_ = other.value_;
  }
  return *this;
}

template <typename T>
template <typename U>
inline const StatusOr<T>& StatusOr<T>::operator=(const StatusOr<U>& other) {
  status_ = other.status_;
  if (status_.ok()) {
    value_ = other.value_;
  }
  return *this;
}

}  // namespace util

#endif  // UTIL_TASK_STATUSOR_H__
