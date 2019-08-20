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
#include <memory>

#include "absl/memory/memory.h"
#include "util/time/clock.h"

namespace util {

class RealClock : public Clock {
public:
  static RealClock *Instance() {
    static RealClock clock;
    return &clock;
  }

  absl::Time TimeNow() override { return absl::Now(); };

  void Sleep(absl::Duration d) override { absl::SleepFor(d); };

  void SleepUntil(absl::Time wakeup_time) override {
    absl::Duration duration = wakeup_time - TimeNow();
    if (duration > absl::ZeroDuration()) {
      Sleep(duration);
    }
  };

  bool AwaitWithDeadline(absl::Mutex *mu, const absl::Condition &cond,
                         absl::Time deadline) override {
    return mu->AwaitWithDeadline(cond, deadline);
  };
};

Clock *Clock::RealClock() { return RealClock::Instance(); }

} // namespace util