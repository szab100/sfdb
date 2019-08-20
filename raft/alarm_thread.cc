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
#include "raft/alarm_thread.h"

#include <functional>

namespace raft {
  using util::thread::Options;
  Options MakeOptions() {
    Options opts;
    opts.set_joinable(true);
    return opts;
  }

using ::absl::Duration;

AlarmThread::AlarmThread(Duration timeout, std::function<void()> on_alarm)
    : Thread(MakeOptions(), "raft_alarm"),
      timeout_(ToChronoMilliseconds(timeout)),
      on_alarm_(on_alarm) {}

void AlarmThread::Stop() {
  q_.push(STOP);
  Join();
}

void AlarmThread::Poke() { q_.push(POKE); }

void AlarmThread::Run() {
  while (true) {
    Command cmd = POKE;
    bool timed_out = false;
    q_.pop(cmd, timeout_, &timed_out);
    if (cmd == STOP) break;
    on_alarm_();
  }
}

}  // namespace raft
