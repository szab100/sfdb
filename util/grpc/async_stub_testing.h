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
#ifndef UTIL_GRPC_ASYNC_STUB_TESTING_H_
#define UTIL_GRPC_ASYNC_STUB_TESTING_H_

#include <memory>
#include <tuple>
#include <type_traits>

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/util/message_differencer.h"
#include "grpcpp/grpcpp.h"
#include "util/grpc/async_stub_wrapper.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace testing {

template <typename Req, typename Response> class RPCCallTestHelper;

template <typename T>
class ResponseReaderMock : public grpc::ClientAsyncResponseReaderInterface<T> {
public:
  MOCK_METHOD0(StartCall, void());
  MOCK_METHOD1(ReadInitialMetadata, void(void *));
  MOCK_METHOD3_T(Finish, void(T *, grpc::Status *, void *));

  ResponseReaderMock()
      : start_call_count_(0), finish_call_count_(0), max_call_count_(1) {
    ON_CALL(*this, StartCall()).WillByDefault(Invoke([this]() -> void {
      EXPECT_LE(++start_call_count_, max_call_count_);
    }));

    ON_CALL(*this, Finish(_, _, _))
        .WillByDefault(Invoke([this](T *responce_out, grpc::Status *status_out,
                                     void *tag) {
          EXPECT_LE(++finish_call_count_, max_call_count_);
          EXPECT_TRUE(start_call_count_ == finish_call_count_);
          CHECK(!!response_ || !status_.ok())
              << "Successfull RPC should have a valid response message";
          if (response_) {
            *responce_out = *response_.get();
          }
          *status_out = status_;
          auto rpc = reinterpret_cast<::util::CompletionCallbackIntf *>(tag);
          rpc->HandleRequestComplete();
        }));
  }

  void SetResponse(const T &response) {
    response_ = absl::make_unique<T>(response);
    status_ = grpc::Status::OK;
  }

  void SetResponse(grpc::Status status) {
    response_ = nullptr;
    status_ = status;
  }

private:
  void SetExpectsCall(size_t count) {
    EXPECT_CALL(*this, StartCall())
        .Times(count)
        .WillRepeatedly(Invoke([this]() -> void { ++start_call_count_; }));

    EXPECT_CALL(*this, Finish(_, _, _))
        .Times(count)
        .WillRepeatedly(Invoke([this](T *responce_out, grpc::Status *status_out,
                                      void *tag) {
          ++finish_call_count_;
          EXPECT_TRUE(start_call_count_ == finish_call_count_);
          CHECK(!!response_ || !status_.ok())
              << "Successfull RPC should have a valid response message";
          if (response_) {
            *responce_out = *response_.get();
          }
          *status_out = status_;
          auto rpc = reinterpret_cast<::util::CompletionCallbackIntf *>(tag);
          rpc->HandleRequestComplete();
        }));
  }

private:
  std::unique_ptr<T> response_;
  grpc::Status status_;
  size_t start_call_count_, finish_call_count_, max_call_count_;

  template <typename Req, typename Response> friend class RPCCallTestHelper;
};

// template to get clean type of argument
template <typename T>
using clean_type_t =
    typename std::remove_reference<typename std::remove_cv<T>::type>::type;

// These template allows to access function argument types by index.
template <typename Res, typename... Args> struct arg_types {
  std::tuple<Args...> &args_pack;

  template <int ArgN> using arg_type_t = decltype(std::get<ArgN>(args_pack));

  template <int ArgN>
  using arg_type_clean_t = clean_type_t<decltype(std::get<ArgN>(args_pack))>;

  using ret_type_t = Res;
  using ret_type_clean_t = clean_type_t<Res>;
};

template <typename T> struct fn_args_type;
template <typename F, typename... Args>
struct fn_args_type<F(Args...)> : arg_types<F, Args...> {};
template <typename C, typename F, typename... Args>
struct fn_args_type<F (C::*)(Args...)> : arg_types<F, Args...> {};

// Template to extract response type from grpc ::PrepareAsync[RPC_NAME]Raw ret
// type which is grpc::ClientAsyncResponseReaderInterface<T>* .
template <typename T>
T *guess_response_type(grpc::ClientAsyncResponseReaderInterface<T> *) {
  return (T *)nullptr;
}
// Template to return type of ResponceReaderMock from grpc
// ::PrepareAsync[RPC_NAME]Raw ret type
template <typename T>
using response_from_response_reader_ptr_t =
    clean_type_t<decltype(*guess_response_type((T) nullptr))>;

// Message container with message set flag
template <typename T> struct OptionalMessage {
  bool message_is_set = false;
  bool result_if_unset = true;
  std::unique_ptr<T> message;
};

// matcher for OptionalMessage message
MATCHER_P(EqualsProtoOpt, other, "") {
  return other->message_is_set
             ? google::protobuf::util::MessageDifferencer::Equals(
                   arg, *other->message)
             : other->result_if_unset;
}

template <typename Req, typename Response> class RPCCallTestHelper {
public:
  // Constructor helps us to capure all required types
  template <typename Expectation>
  RPCCallTestHelper(
      std::unique_ptr<OptionalMessage<Req>> &&req,
      std::unique_ptr<ResponseReaderMock<Response>> &&response_reader,
      Expectation &e)
      : req_(std::move(req)), response_reader_(std::move(response_reader)) {
    e.WillByDefault(Return(response_reader_.get()));
  }

  ResponseReaderMock<Response> *GetResponseReader() {
    CHECK(!!response_reader_);

    return response_reader_.get();
  }

  RPCCallTestHelper &WithRequest(const Req &req) {
    CHECK(!!req_);

    req_->message_is_set = true;
    req_->message = absl::make_unique<Req>(req);

    return *this;
  }

  RPCCallTestHelper &WithResponse(const Response &response) {
    CHECK(!!response_reader_);
    response_reader_->SetResponse(response);

    return *this;
  }

  RPCCallTestHelper &WithResponseError(grpc::Status status) {
    CHECK(!!response_reader_);
    response_reader_->SetResponse(status);

    return *this;
  }

  RPCCallTestHelper &Times(size_t times) {
    CHECK(!!response_reader_);
    response_reader_->SetExpectsCall(times);

    return *this;
  }

private:
  std::unique_ptr<OptionalMessage<Req>> req_;
  std::unique_ptr<ResponseReaderMock<Response>> response_reader_;
};
} // namespace testing

// Beware, very dangerous macro magic!

#define CONCATENATE_DETAIL(x, y) x##y
#define CONCATENATE(x, y) CONCATENATE_DETAIL(x, y)
#define MAKE_UNIQUE(x) CONCATENATE(x, __COUNTER__)

#define MAKE_RPC_ASYNC_FN_NAME(call_name) PrepareAsync##call_name##Raw

// Longish template just signature of
// Mock[SERVICE_NAME]Stub::PrepareAsync[RPC_NAME]Raw and parses it for arguments
// types from which it extracts request type. uniq1, uniq2, uniq3 are uniq tags
// to be used as type names or variables
#define EXPECT_ASYNC_PREPARE_RPC_DETAIL(uniq1, uniq2, uniq3, uniq4, mock,      \
                                        call_name)                             \
  /*uniq1 is type traits focontaining ::PrepareAsync[RPC_NAME]Raw ret type and \
   * arg types, see  FnArgs*/                                                  \
  using uniq1 = testing::fn_args_type<decltype(                                \
      &std::remove_reference<decltype((mock))>::type ::call_name)>;            \
  /*uniq2 is a placeholder var for request message if set*/                    \
  auto uniq2 = absl::make_unique<                                              \
      testing::OptionalMessage<typename uniq1::arg_type_clean_t<1>>>();        \
  /*uniq3 is a placeholder var for response reader, it is used in Response     \
   * type deduction of RPCCallTestHelper*/                                     \
  auto uniq3 = absl::make_unique<testing::ResponseReaderMock<                  \
      testing::response_from_response_reader_ptr_t<                            \
          typename uniq1::ret_type_clean_t>>>();                               \
  /*uniq4 is a helper class which allows to set expecations on                 \
   * request/response proto messages, request satus codes*/                    \
  auto uniq4 =                                                                 \
      testing::RPCCallTestHelper<typename uniq1::arg_type_clean_t<1>,          \
                                 testing::response_from_response_reader_ptr_t< \
                                     typename uniq1::ret_type_clean_t>>(       \
          std::move(uniq2), std::move(uniq3),                                  \
          ON_CALL(mock,                                                        \
                  call_name(testing::_, testing::EqualsProtoOpt(uniq2.get()),  \
                            testing::_)));                                     \
  uniq4

#define EXPECT_ASYNC_RPC1(mock, rcp_name)                                      \
  EXPECT_ASYNC_PREPARE_RPC_DETAIL(MAKE_UNIQUE(u1), MAKE_UNIQUE(u2),            \
                                  MAKE_UNIQUE(u3), MAKE_UNIQUE(u4), mock,      \
                                  MAKE_RPC_ASYNC_FN_NAME(rcp_name))

#endif // UTIL_GRPC_ASYNC_STUB_TESTING_H_
