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
#ifndef THREAD_THREAD_H_
#define THREAD_THREAD_H_

#include <memory>
#include <string>
#include <thread>

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "util/macros/macros.h"

namespace util::thread {
class Options {
public:
  void set_joinable(bool joinable) { joinable_ = joinable; }
  bool joinable() const { return joinable_; }

private:
  bool joinable_;
};
} // namespace util::thread

class Thread {
public:
  Thread();
  virtual ~Thread();

  Thread(const util::thread::Options &options, absl::string_view name_prefix);
  const util::thread::Options &options() const;
  const std::string &name_prefix() const;

  void Start();
  void Join();

protected:
  virtual void Run() = 0;

private:
  bool created_;
  bool needs_join_;
  util::thread::Options options_;
  std::string name_prefix_;
  std::thread thread_;

  static void *ThreadBody(void *arg);

  DISALLOW_COPY_AND_ASSIGN(Thread);
};

#endif // THREAD_THREAD_H_