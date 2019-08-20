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
#include "raft/msg_ids.h"

#include "absl/time/time.h"
#include "glog/logging.h"
#include "util/time/clock.h"

namespace raft {

MsgIds::MsgIds(int n, int k) : k_(k) {
  CHECK(n <= 16);
  CHECK(0 <= k && k < n);
}

uint64 MsgIds::Make() {
  // An ID is made up of these 3 parts, in decreasing order of significance:
  // - 50 bits of microsecond timestamp (cycles after 35.7 years).
  // - 10 bits of a monotonically increasing counter (per_micro_).
  // - 4 bits of replica ID (k_).

  const ::absl::Time t = ::util::Clock::RealClock()->TimeNow();
  const uint64 micros = ::absl::ToUnixMicros(t) & ((1ULL << 50) - 1);
  ::absl::MutexLock lock(&mu_);
  if (micros > prev_micros_ || prev_micros_ - micros > (1ULL << 49)) {
    // time went forward by at least a microsecond
    prev_micros_ = micros;
    per_micro_ = 0;
  } else {
    // time went backwards or stayed at the same microsecond
    CHECK(++per_micro_ < 1024);
  }
  return (((prev_micros_ << 10) | per_micro_) << 4) | k_;
}

}  // namespace raft
