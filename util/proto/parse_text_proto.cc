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
#include <vector>

#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"
#include "util/proto/parse_text_proto.h"

namespace util {
namespace {
using namespace ::google::protobuf;

class DefaultErrorCollector : public io::ErrorCollector {
public:
  void AddError(int line, int column, const string &message) override {}

  void AddWarning(int line, int column, const string &message) override {}

  // TODO: implement
  const std::string errors() const { return ""; }
};
} // namespace

namespace internal {
bool ParseTextProto(absl::string_view asciipb, ParserConfig config,
                    SourceLocation loc, Message *msg, string *errors) {
  TextFormat::Parser parser;
  parser.AllowPartialMessage(config.allow_partial_messages);
  parser.AllowUnknownExtension(config.allow_unknown_extensions);
  DefaultErrorCollector error_collector;
  parser.RecordErrorsTo(&error_collector);

  io::ArrayInputStream asciipb_istream(asciipb.data(), asciipb.size());
  if (parser.Parse(&asciipb_istream, msg)) {
    return true;
  }

  msg->Clear();

  if (errors) {
    *errors = error_collector.errors();
  }

  return false;
}
} // namespace internal
} // namespace util
