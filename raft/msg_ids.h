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
#ifndef RAFT_MSG_IDS_H_
#define RAFT_MSG_IDS_H_

#include "absl/synchronization/mutex.h"
#include "util/types/integral_types.h"

namespace raft {

// Generates unique message IDs.
//
// Assumptions:
// - ~100k/second is enough; optimize this if needed.
// - Monotonicity is not a requirement.
// - A binary cannot be restarted in less than 1 microsecond.
// - n <= 16
// - 0 <= k < n
// - Clock::RealClock() has at least microsecond precision.
// - Two ID streams created with the same (n, k) pair may collide.
// - The same MsgIds object may start repeating IDs after 35 years of use.
//
// Thread-safe.
class MsgIds {
 public:
  // Creates a message ID generator for the k'th member of an n-member cluster.
  MsgIds(int n, int k);

  // Makes a unique, non-zero message ID.
  uint64 Make();

 private:
  const int k_;
  absl::Mutex mu_;
  uint64 prev_micros_ GUARDED_BY(mu_) = 0;
  uint64 per_micro_ GUARDED_BY(mu_) = 0;
};

}  // namespace raft

#endif  // RAFT_MSG_IDS_H_
