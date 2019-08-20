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
#ifndef RAFT_ALARM_THREAD_H_
#define RAFT_ALARM_THREAD_H_

#include <chrono>
#include <functional>

#include "absl/time/time.h"
#include "util/thread/thread.h"
#include "util/thread/concurrent_queue.h"

namespace raft {

// The periodic alarm thread that triggers all actions of the RAFT algorithm
// that aren't a direct result of an incoming RPC.
//
// Wakes up periodically or in response to Poke(), and calls ServiceImpl's
// OnAlarm().
//
// Thread-safe.
class AlarmThread : public Thread {
 public:
  AlarmThread(::absl::Duration timeout, std::function<void()> on_alarm);

  ~AlarmThread() override = default;
  AlarmThread(const AlarmThread&) = delete;
  AlarmThread &operator=(const AlarmThread&) = delete;

  void Poke();
  void Stop();  // if you call Start(), call Stop().

 protected:
  void Run() override;

 private:
  const std::chrono::milliseconds timeout_;
  const std::function<void()> on_alarm_;

  enum Command { POKE, STOP };
  WaitQueue<Command> q_;
};

}  // namespace raft

#endif  // RAFT_ALARM_THREAD_H_
