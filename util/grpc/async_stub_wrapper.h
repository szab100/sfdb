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
#ifndef UTIL_GRPC_ASYNC_STUB_WRAPPER_H_
#define UTIL_GRPC_ASYNC_STUB_WRAPPER_H_

#include <inttypes.h>

#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "grpcpp/completion_queue.h"
#include "util/grpc/async_response_reader.h"

namespace util {
#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

// Macro to be used toagather with GRPCAsyncStubWrapper to simplify handling
// async calls.
#define DEFINE_ASYNC_GRPC_CALL(call_name, req_type, response_type)             \
  std::unique_ptr<::util::GRPCAsyncResponseReader<response_type>> call_name(   \
      const req_type &req, ::util::ResponseCallback cb) {                      \
    auto rpc =                                                                 \
        absl::make_unique<::util::GRPCAsyncResponseReader<response_type>>(     \
            ::util::RequestOptions(), cb);                                     \
    VLOG(5) << absl::StrFormat(                                                \
        "New async request " TOSTRING(call_name) "(%" PRId64 ")",              \
        reinterpret_cast<uint64_t>(rpc.get()));                                \
    auto reader = stub_->PrepareAsync##call_name(rpc->context(), req, &cq_);   \
    rpc->Start(std::move(reader));                                             \
    return rpc;                                                                \
  }

#define DEFINE_SYNC_GRPC_CALL(call_name, req_type, response_type)              \
  std::unique_ptr<::util::GRPCSyncedResponseReader<response_type>> call_name(  \
      const req_type &req) {                                                   \
    auto rpc =                                                                 \
        absl::make_unique<::util::GRPCSyncedResponseReader<response_type>>(    \
            ::util::RequestOptions());                                         \
    VLOG(5) << absl::StrFormat(                                                \
        "New synced request " TOSTRING(call_name) "(%" PRId64 ")",             \
        reinterpret_cast<uint64_t>(rpc.get()));                                \
    auto reader = stub_->PrepareAsync##call_name(rpc->context(), req, &cq_);   \
    rpc->Start(std::move(reader));                                             \
    return rpc;                                                                \
  }

template <typename T> class AsyncStubWrapper {
public:
  AsyncStubWrapper(std::unique_ptr<T> &&stub, size_t num_dispatch_threads = 1)
      : stub_(std::move(stub)) {
    for (size_t i = 0; i < num_dispatch_threads; ++i) {
      dispatch_threads_.emplace_back(&AsyncStubWrapper<T>::DispatchThreaMain,
                                     this);
    }
  }

  virtual ~AsyncStubWrapper() {
    cq_.Shutdown();
    for (auto &t : dispatch_threads_) {
      if (t.joinable()) {
        t.join();
      }
    }
  };

protected:
  std::unique_ptr<T> stub_;
  grpc::CompletionQueue cq_;
  std::vector<std::thread> dispatch_threads_;

  static void DispatchThreaMain(void *arg);
};

template <typename T> void AsyncStubWrapper<T>::DispatchThreaMain(void *arg) {
  auto this_ = reinterpret_cast<AsyncStubWrapper<T> *>(arg);

  void *got_tag;
  bool ok = false;

  // Block until the next result is available in the completion queue "cq".
  while (this_->cq_.Next(&got_tag, &ok)) {
    // TODO: this is not portable, works only on 64bit systems
    VLOG(5) << absl::StrFormat("Got response %" PRId64,
                               reinterpret_cast<uint64_t>(got_tag));
    GPR_ASSERT(ok);

    // The tag in this example is the memory location of the call object
    auto rpc = static_cast<CompletionCallbackIntf *>(got_tag);
    CHECK(rpc);

    // Let call object manage itself.
    rpc->HandleRequestComplete();
  }
}

} // namespace util

#endif // UTIL_GRPC_ASYNC_STUB_WRAPPER_H_