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

#include <memory>
#include <set>
#include <vector>

#include "absl/memory/memory.h"
#include "gtest/gtest.h"
#include "util/thread/concurrent_queue.h"
#include "util/thread/thread.h"
#include "util/types/integral_types.h"

namespace raft {

using ::absl::make_unique;
using util::thread::Options;

const int kNumThreads = 4;
const int kIdsPerThread = 40000;
uint64 ids[kNumThreads][kIdsPerThread];

Options Opts() {
  Options opts;
  opts.set_joinable(true);
  return opts;
}

// Generates kIdsPerThread IDs and appends them to *out.
class T : public Thread {
 public:
  T(MsgIds *gen, uint64 *out) : Thread(Opts(), "T"), gen_(gen), out_(out) {
    Start();
  }
  void Run() override {
    for (int i = 0; i < kIdsPerThread; ++i) out_[i] = gen_->Make();
  }

 private:
  MsgIds *gen_;
  uint64 *out_;
};

TEST(MsgIdsTest, Uniqueness) {
  MsgIds gen(5, 2);

  std::vector<std::unique_ptr<T>> threads;
  for (int i = 0; i < kNumThreads; ++i)
    threads.emplace_back(make_unique<T>(&gen, ids[i]));
  for (int i = 0; i < kNumThreads; ++i) threads[i]->Join();

  std::set<uint64> all_ids;
  for (int i = 0; i < kNumThreads; ++i)
    all_ids.insert(ids[i], ids[i] + kIdsPerThread);
  ASSERT_EQ(kNumThreads * kIdsPerThread, all_ids.size());
}

}  // namespace raft
