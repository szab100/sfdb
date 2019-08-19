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
#ifndef UTIL_PROTO_PARSE_TEXT_PROTO_H_
#define UTIL_PROTO_PARSE_TEXT_PROTO_H_

#include <string>

#include "absl/strings/string_view.h"
#include "glog/logging.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace util {
struct ParserConfig {
  bool allow_partial_messages = false;
  bool allow_unknown_extensions = false;
};

class SourceLocation {
public:
  SourceLocation(uint32_t line, const char* file_name) : line_(line), file_name_(file_name) {}
  constexpr uint32_t line() const { return line_; }
  constexpr const char* file_name() const { return file_name_; }
private:
  uint32_t line_;
  const char* file_name_;
};

namespace internal {
bool ParseTextProto(absl::string_view asciipb, ParserConfig config, SourceLocation loc, google::protobuf::Message* msg, std::string* errors);
} // namespace internal

// NOTE: asciipb should have broader scope than instance of ParseTextProtoHelper
class ParseTextProtoHelper {
public:
  template <typename T>
  T Parse() const {
    T result;
    std::string errors;
    CHECK(internal::ParseTextProto(text_, config_, loc_, &result, &errors)) << errors;
    return result;
  }

  template <typename T>
  operator T() const {
    return Parse<T>();
  }
private:
  ParseTextProtoHelper(absl::string_view asciipb, ParserConfig config, SourceLocation loc)
  : text_(asciipb), config_(config), loc_(loc) {}
private:
  absl::string_view text_;
  ParserConfig config_;
  SourceLocation loc_;

  friend ParseTextProtoHelper ParseTextProtoOrDie(absl::string_view asciipb, ParserConfig config, SourceLocation loc);
};

ParseTextProtoHelper ParseTextProtoOrDie(absl::string_view asciipb, ParserConfig config, SourceLocation loc) {
  return ParseTextProtoHelper(asciipb, config, loc);
}

}

#define PARSE_TEST_PROTO(asciipb) \
  ::util::ParseTextProtoOrDie(asciipb, {}, ::util::SourceLocation(__LINE__, __FILE__))

#endif // UTIL_PROTO_PARSE_TEXT_PROTO_H_