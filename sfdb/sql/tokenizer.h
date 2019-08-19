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
#ifndef SFDB_SQL_TOKENIZER_H_
#define SFDB_SQL_TOKENIZER_H_

#include <ostream>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"
#include "util/task/statusor.h"
#include "util/types/integral_types.h"

namespace sfdb {

// A SQL statement token.
//
// Immutable and self-contained.
struct Token {
  enum Type {
    ERROR = 0,
    WORD,  // SELECT, TABLE, AND, etc.
    INT64,  // 13, -4, 0xdeadbeef, 0666
    DOUBLE,  // 1.3, 9.3e-5
    QUOTED_STRING,  // "hi", 'sup'
    PAREN_OPEN,  // (
    PAREN_CLOSE,  // )
    BRACKET_OPEN,  // [
    BRACKET_CLOSE,  // ]
    COMMA,  // ,
    SEMICOLON,  // ;
    STAR,  // *
    DOT,  // .
    EQ,  // =
    LT,  // <
    GT,  // >
    LE,  // <=
    GE,  // >=
    NE,  // <>
    PLUS,  // +
    MINUS,  // -
    TILDE,  // ~
    AMPERSAND,  // &
    PIPE,  // |
    CARET,  // ^
    SLASH,  // /
    PERCENT,  // %
  };

  // these are always set
  const Type type;
  // TODO: Use uint64 ?
  const uint32 offset;  // where it begins in the input string
  const uint32 len;     // length in the input string, if known

  // at most one of these is set
  const std::string error;  // when type is ERROR
  const std::string word;   // when type is WORD
  const std::string str;    // the unescaped QUOTED_STRING
  const int64 i64;     // the INT64 value
  const double dbl;    // the DOUBLE value

  static Token Word(uint32 offset, uint32 len, ::absl::string_view full_sql);
  static Token QuotedString(uint32 offset, uint32 len, ::absl::string_view str);
  static Token Int64(uint32 offset, uint32 len, int64 value);
  static Token Double(uint32 offset, uint32 len, double value);

  Token() = delete;

  // Returns the human readable position of this token in the SQL statement.
  std::string GetPositionIn(::absl::string_view sql) const;

  std::string ToString() const;
};

bool operator==(const Token &a, const Token &b);
::std::ostream &operator<<(::std::ostream &out, const Token &t);

::util::StatusOr<::std::vector<Token>> Tokenize(::absl::string_view sql);

}  // namespace sfdb

#endif  // SFDB_SQL_TOKENIZER_H_
