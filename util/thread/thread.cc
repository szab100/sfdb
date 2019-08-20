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
#include "thread.h"

#include <thread>

#include "glog/logging.h"

Thread::Thread() : created_(false), needs_join_(false) {}

Thread::~Thread() {
  if (needs_join_) {
    LOG(ERROR) << "Joinable thread was never joined.";
  }
}

Thread::Thread(const util::thread::Options &options, absl::string_view name_prefix)
    : created_(false), needs_join_(false), options_(options),
      name_prefix_(name_prefix) {}

const util::thread::Options &Thread::options() const { return options_; }

const std::string &Thread::name_prefix() const { return name_prefix_; }

void Thread::Start() {
  created_ = true;
  needs_join_ = options_.joinable();

  thread_ = std::thread(&Thread::ThreadBody, this);
  if (!options_.joinable()) {
    thread_.detach();
  }
  needs_join_ = false;
}

void Thread::Join() {
  CHECK(options_.joinable());
  CHECK(created_);
  thread_.join();
}

void *Thread::ThreadBody(void *arg) {
  Thread *this_thread = reinterpret_cast<Thread *>(arg);
  this_thread->Run();

  return arg;
}
