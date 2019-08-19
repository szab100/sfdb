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
#include "sfdb/sql/tokenizer.h"

#include <ctype.h>

#include <string>
#include <vector>
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "util/task/canonical_errors.h"

namespace sfdb {
namespace {

using ::absl::StrCat;
using ::absl::numbers_internal::safe_strtou64_base;
using ::absl::string_view;
using ::util::InvalidArgumentError;
using ::util::StatusOr;

inline bool IsWhitespace(char c) {
  switch (c) {
    case ' ': case '\t': case '\n': case '\r': return true;
  }
  return false;
}

inline bool IsWordChar(char c) {
  return isalnum(c) || c == '_';
}

inline bool IsValidQuotedChar(char c) {
  return 32 <= c && c < 127;
}

inline void SkipWhitespace(string_view sql, uint32 *i) {
  while (*i < sql.size() && IsWhitespace(sql[*i])) ++*i;
}

// Converts \c into the char it represents. Returns false on error.
inline bool UnbackslashChar(char c, char *out) {
  switch (c) {
    case '0': *out = '\0'; break;
    case 'b': *out = '\b'; break;
    case 'n': *out = '\n'; break;
    case 'r': *out = '\r'; break;
    case 't': *out = '\t'; break;
    case '"': *out = '\"'; break;
    case '\'': *out = '\''; break;
    default: return false;
  }
  return true;
}

// Invariant: sql[*i] is a letter.
Token ParseWord(string_view sql, uint32 *i) {
  const uint32 j = *i;
  while (*i < sql.size() && IsWordChar(sql[*i])) ++*i;
  return Token::Word(j, *i - j, sql);
}

// Invariant: the previous two characters spell out "0x".
Token ParseHexInt(string_view sql, uint32 *i) {
  const uint32 j = --*i - 1;
  while (++*i < sql.size()) {
    if (isdigit(sql[*i])) continue;
    if (isalpha(sql[*i]) && toupper(sql[*i]) <= 'F') continue;
    break;
  }
  if (*i < sql.size() && isalpha(sql[*i])) {
    return {Token::ERROR, j, *i - j, StrCat(
        "Unexpected '", sql.substr(*i, 1), "'")};
  }
  uint64_t value;
  if (!safe_strtou64_base(sql.substr(j + 2, *i - j - 2), &value, 16))
    return {Token::ERROR, j, *i - j, "Bad hex integer"};
  return Token::Int64(j, *i - j, static_cast<int64>(value));
}

// Invariant: sql[*i] is a digit.
Token ParseNumber(string_view sql, uint32 *i) {
  const uint32 j = *i;

  // deal with hex integers separately
  if (j + 2 < sql.size() && sql[j] == '0' && sql[j + 1] == 'x') {
    *i += 2;
    return ParseHexInt(sql, i);
  }

  // find anything that looks like a number
  bool must_be_float = false;
  while (++*i < sql.size()) {
    if (isdigit(sql[*i])) continue;
    if (sql[*i] == '.' || sql[*i] == 'e' || sql[*i] == 'E') {
      must_be_float = true;
      continue;
    }
    if (isalpha(sql[*i]))
      return {Token::ERROR, *i, 1, StrCat(
          "Unexpected '", sql.substr(*i, 1), "'")};
    break;
  }

  // float
  if (must_be_float) {
    double value;
    if (!::absl::SimpleAtod(sql.substr(j, *i - j), &value))
      return {Token::ERROR, j, *i - j, "Bad float"};
    return Token::Double(j, *i - j, value);
  }

  uint64_t value;

  // octal
  if (*i - j > 1 && sql[j] == '0') {
    if (!safe_strtou64_base(sql.substr(j, *i - j), &value, 8))
      return {Token::ERROR, j, *i - j, "Bad octal integer"};
    return Token::Int64(j, *i - j, static_cast<int64>(value));
  }

  // decimal
  if (!safe_strtou64_base(sql.substr(j, *i - j), &value, 10)) {
    return {Token::ERROR, j, *i - j, "Bad integer"};
  }

  if (value > 0x7FFFFFFFFFFFFFFF) {
    return {Token::ERROR, j, *i - j, "int64 overflow"};
  }

  return Token::Int64(j, *i - j, static_cast<int64>(value));
}

// Invariant: sql[*i] is ' or ".
Token ParseQuotedString(string_view sql, uint32 *i) {
  const uint32 j = *i;
std::string str;
  while (++*i < sql.size()) {
    if (sql[*i] == sql[j])
      return Token::QuotedString(j, ++(*i) - j, str);
    if (sql[*i] == '\\') {
      if (++*i >= sql.size()) return {Token::ERROR, j, *i - j, "Bad backslash"};
      char c;
      if (!UnbackslashChar(sql[*i], &c))
        return {Token::ERROR, j, *i + 1 - j, StrCat(
            "Unexpected ", sql.substr(*i, 1), " after backslash")};
      str += c;
    } else if (IsValidQuotedChar(sql[*i])) {
      str += sql[*i];
    } else {
      return {Token::ERROR, *i, 1, StrCat(
          "Invalid quoted character: '", sql.substr(*i, 1), "'")};
    }
  }
  return {Token::ERROR, j, *i - j, "Unterminated string literal"};
}

// Parses one token.
// Invariant: *i < sql.size() and is not a whitespace.
Token ParseToken(string_view sql, uint32 *i) {
  const char c = sql[*i];
  if (isalpha(c)) return ParseWord(sql, i);
  if (isdigit(c)) return ParseNumber(sql, i);
  if (c == '\'') return ParseQuotedString(sql, i);
  if (c == '"') return ParseQuotedString(sql, i);
  if (c == '(') return {Token::PAREN_OPEN, (*i)++, 1};
  if (c == ')') return {Token::PAREN_CLOSE, (*i)++, 1};
  if (c == '[') return {Token::BRACKET_OPEN, (*i)++, 1};
  if (c == ']') return {Token::BRACKET_CLOSE, (*i)++, 1};
  if (c == ',') return {Token::COMMA, (*i)++, 1};
  if (c == ';') return {Token::SEMICOLON, (*i)++, 1};
  if (c == '*') return {Token::STAR, (*i)++, 1};
  if (c == '.') return {Token::DOT, (*i)++, 1};
  if (c == '=') return {Token::EQ, (*i)++, 1};
  if (c == '+') return {Token::PLUS, (*i)++, 1};
  if (c == '-') return {Token::MINUS, (*i)++, 1};
  if (c == '~') return {Token::TILDE, (*i)++, 1};
  if (c == '&') return {Token::AMPERSAND, (*i)++, 1};
  if (c == '|') return {Token::PIPE, (*i)++, 1};
  if (c == '^') return {Token::CARET, (*i)++, 1};
  if (c == '/') return {Token::SLASH, (*i)++, 1};
  if (c == '%') return {Token::PERCENT, (*i)++, 1};
  if (c == '<') {
    if (*i + 1 < sql.size()) {
      if (sql[*i + 1] == '=') {
        (*i) += 2;
        return {Token::LE, *i - 2, 2};
      }
      if (sql[*i + 1] == '>') {
        (*i) += 2;
        return {Token::NE, *i - 2, 2};
      }
    }
    return {Token::LT, (*i)++, 1};
  }
  if (c == '>') {
    if (*i + 1 < sql.size() && sql[*i + 1] == '=') {
      (*i) += 2;
      return {Token::GE, *i - 2, 2};
    }
    return {Token::GT, (*i)++, 1};
  }
  return {Token::ERROR, *i, 1, StrCat(
      "Unexpected character '", string_view(&c, 1), "'")};
}

}  // namespace

// static
Token Token::Word(uint32 offset, uint32 len, string_view full_sql) {
  return {Token::WORD, offset, len, "", std::string(full_sql.substr(offset, len))};
}

// static
Token Token::QuotedString(uint32 offset, uint32 len, string_view str) {
  return {Token::QUOTED_STRING, offset, len, "", "", std::string(str)};
}

// static
Token Token::Int64(uint32 offset, uint32 len, int64 value) {
  return {Token::INT64, offset, len, "", "", "", value};
}

// static
Token Token::Double(uint32 offset, uint32 len, double value) {
  return {Token::DOUBLE, offset, len, "", "", "", 0, value};
}

bool operator==(const Token &a, const Token &b) {
  return a.type == b.type
      && a.offset == b.offset
      && a.len == b.len
      && a.error == b.error
      && a.word == b.word
      && a.str == b.str
      && a.i64 == b.i64
      && a.dbl == b.dbl;
}

std::string Token::GetPositionIn(string_view sql) const {
  uint32 line = 1, last_endl = 0;
  for (uint32 j = 0; j < offset; ++j) {
    if (sql[j] == '\n') {
      ++line;
      last_endl = j;
    }
  }
  return StrCat("line ", line, ", byte ", offset - last_endl);
}

std::string Token::ToString() const {
  switch (type) {
    case Token::ERROR: return StrCat("error(", error, ")");
    case Token::WORD: return word;
    case Token::INT64: return StrCat("int(", i64, ")");
    case Token::DOUBLE: return StrCat("double(", dbl, ")");
    case Token::QUOTED_STRING: return StrCat("str(", str, ")");
    case Token::PAREN_OPEN: return "(";
    case Token::PAREN_CLOSE: return ")";
    case Token::BRACKET_OPEN: return "[";
    case Token::BRACKET_CLOSE: return "]";
    case Token::COMMA: return ",";
    case Token::SEMICOLON: return ";";
    case Token::STAR: return "*";
    case Token::DOT: return ".";
    case Token::EQ: return "=";
    case Token::LT: return "<";
    case Token::GT: return ">";
    case Token::LE: return "<=";
    case Token::GE: return ">=";
    case Token::NE: return "<>";
    case Token::PLUS: return "+";
    case Token::MINUS: return "-";
    case Token::TILDE: return "~";
    case Token::AMPERSAND: return "&";
    case Token::PIPE: return "|";
    case Token::CARET: return "^";
    case Token::SLASH: return "/";
    case Token::PERCENT: return "%";
    default: LOG(FATAL) << "Invalid toke type " << type
      << " in Token::ToString()";
  }
}

::std::ostream &operator<<(::std::ostream &out, const Token &t) {
  return out << t.ToString();
}

StatusOr<::std::vector<Token>> Tokenize(string_view sql) {
  ::std::vector<Token> tokens;
  uint32 i = 0;
  while (true) {
    SkipWhitespace(sql, &i);
    if (i == sql.size()) break;
    tokens.push_back(ParseToken(sql, &i));
    if (tokens.back().type == Token::ERROR) return InvalidArgumentError(StrCat(
        "Parse error at ", tokens.back().GetPositionIn(sql), ": ",
        tokens.back().error));
  }
  return tokens;
}

}  // namespace sfdb
