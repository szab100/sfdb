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
#ifndef UTIL_TIME_CLOCK_H_
#define UTIL_TIME_CLOCK_H_

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

namespace util {
class Clock {
public:
  static Clock *RealClock();

  virtual ~Clock() = default;

  virtual absl::Time TimeNow() = 0;

  virtual void Sleep(absl::Duration d) = 0;

  virtual void SleepUntil(absl::Time wakeup_time) = 0;

  virtual bool AwaitWithDeadline(absl::Mutex *mu, const absl::Condition &cond,
                                 absl::Time deadline) = 0;

protected:
  Clock() = default;
};
} // namespace util

#endif // UTIL_TIME_CLOCK_H_