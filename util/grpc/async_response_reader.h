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
#ifndef UTIL_GRPC_ASYNC_RESPONSE_READER_H_
#define UTIL_GRPC_ASYNC_RESPONSE_READER_H_

#include <functional>
#include <memory>
#include <thread>

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "util/grpc/completion_callback.h"

namespace util {

struct RequestOptions {
  absl::Duration deadline;
  bool delete_after_completion = true;
};

// Common base for classes implementing wrappers for GRPC asynchronus calls.
// It stores core members used by all classes dealing with async calls.
template <typename T>
struct AsyncResponseReader : public CompletionCallbackIntf {
  using ReaderType = typename grpc::ClientAsyncResponseReaderInterface<T>;
  using ContextType = grpc::ClientContext;

  AsyncResponseReader() = delete;

  AsyncResponseReader(const RequestOptions &opts)
      : started_(false), completed_(false), opts_(opts),
        context_(absl::make_unique<ContextType>()) {
    // TODO: implement
    // context_->set_deadline(opts_.deadline);
  }

  ContextType *context() { return context_.get(); }

  const ContextType *context() const { return context_.get(); }

  void Start(std::unique_ptr<ReaderType> &&reader) {
    started_ = true;
    response_reader_ = std::move(reader);
    response_reader_->StartCall();
    response_reader_->Finish(&response_, &status_, this);
  }

  ~AsyncResponseReader() { CHECK(completed_ || !started_); }

protected:
  void HandleRequestComplete() override {
    LOG(FATAL) << "HandleRequestComplete is not overriden";
  }

  bool started_, completed_;
  RequestOptions opts_;

  std::unique_ptr<grpc::ClientContext> context_;
  std::unique_ptr<ReaderType> response_reader_;
  grpc::Status status_;
  T response_;
};

// Wrapper class for async responces which can be waited on.
// TODO: make less confusing name.
// Response can be retrieved via Status() and Message() if Await() returned true
template <typename T>
struct GRPCSyncedResponseReader : public AsyncResponseReader<T> {
  GRPCSyncedResponseReader(const RequestOptions &opts)
      : AsyncResponseReader<T>(opts) {}

  // If await returns false results are invalid.
  // TODO: add default value?
  bool Await(/*absl::Duration d*/) {
    CHECK(this->started_);
    absl::MutexLock l(&mu_);
    auto cond = [this]() { return this->completed_; };
    mu_.Await(absl::Condition(&cond));

    return true;
  }

  grpc::Status Status() const {
    CHECK(this->started_ && this->completed_);
    return this->status_;
  }

  const T &Message() const {
    CHECK(this->started_ && this->completed_);
    return this->response_;
  }

protected:
  void HandleRequestComplete() override {
    VLOG(5) << absl::StrFormat("Synced request %" PRId64 " completed",
                               reinterpret_cast<uint64_t>(this));

    CHECK(this->started_);

    absl::MutexLock l(&mu_);
    this->completed_ = true;
  }

private:
  absl::Mutex mu_;
};

using ResponseCallback = std::function<void(
    grpc::Status status, const google::protobuf::Message *message)>;

// Wrapper class for asynchronus GRPC requests which will call callback
// functions when it is completed.
template <typename T>
class GRPCAsyncResponseReader : public AsyncResponseReader<T> {
public:
  GRPCAsyncResponseReader(const RequestOptions &opts,
                          const ResponseCallback &cb)
      : AsyncResponseReader<T>(opts), callback_(cb) {}

protected:
  void HandleRequestComplete() override {
    this->completed_ = true;

    VLOG(5) << absl::StrFormat("Async request %" PRId64 " completed",
                               reinterpret_cast<uint64_t>(this));
    callback_(this->status_, &this->response_);
  }

private:
  ResponseCallback callback_;
};

} // namespace util

#endif // UTIL_GRPC_ASYNC_RESPONSE_READER_H_