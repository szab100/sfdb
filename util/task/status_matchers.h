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
#ifndef UTIL_TASK_STATUS_MATCHER_H_
#define UTIL_TASK_STATUS_MATCHER_H_

#include "util/task/statusor.h"
#include <gmock/gmock-matchers.h>

namespace testing {
namespace status {

template <typename T>
inline const ::util::Status& GetStatus(const ::util::StatusOr<T>& status) {
  return status.status();
};

inline const ::util::Status& GetStatus(const ::util::Status& status) {
  return status;
};

template <typename T>
class MonoIsOkMatcherImpl : public MatcherInterface<T> {
 public:
  void DescribeTo(std::ostream* os) const override { *os << "is OK"; }
  void DescribeNegationTo(std::ostream* os) const override {
    *os << "is not OK";
  }
  bool MatchAndExplain(T actual_value, MatchResultListener*) const override {
    return GetStatus(actual_value).ok();
  }
};

class IsOkMatcher {
 public:
  template <typename T>
  operator Matcher<T>() const {  // NOLINT
    return Matcher<T>(new MonoIsOkMatcherImpl<T>());
  }
};

inline IsOkMatcher IsOk() { return IsOkMatcher(); }

}  // namespace status
}  // namespace testing

#define EXPECT_OK(expression) EXPECT_THAT(expression, ::testing::status::IsOk())
#define ASSERT_OK(expression) ASSERT_THAT(expression, ::testing::status::IsOk())

#endif  // UTIL_TASK_STATUS_MATCHER_H_
