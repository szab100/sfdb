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
#include "sfdb/sql/parser.h"

#include <memory>
#include <string>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "sfdb/sql/tokenizer.h"
#include "util/task/canonical_errors.h"
#include "util/types/integral_types.h"

namespace sfdb {
namespace {

using ::absl::AsciiStrToLower;
using ::absl::AsciiStrToUpper;
using ::absl::StrCat;
using ::absl::string_view;
using ::util::InvalidArgumentError;
using ::util::OkStatus;
using ::util::Status;
using ::util::StatusOr;

// The state of the parser while processing one SQL program.
//
// Contains a reference to the original SQL input, and its token list.
// Also keeps an index to the next unprocessed token during parsing.
struct Parser {
  const string_view sql;
  const std::vector<Token> tokens;
  uint32 i;

  inline const Token &LastToken() const { return tokens[i - 1]; }
  inline std::string LastTokenStr() const { return LastToken().ToString(); }

  inline uint32 CurrOffset() const {
    if (tokens.empty()) return 0;
    if (i >= tokens.size()) return tokens.back().offset + tokens.back().len;
    return tokens[i].offset;
  }

  inline bool NextTokenIs(Token::Type type) const {
    return i < tokens.size() && tokens[i].type == type;
  }

  inline bool NextTokenIsUpWord(string_view upword) const {
    return NextTokenIs(Token::WORD)
        && upword == AsciiStrToUpper(tokens[i].word);
  }

  inline bool NextTokenIsOp(Ast::Type op) const {
    if (i >= tokens.size()) return false;
    switch (op) {
      case Ast::OP_IN: return NextTokenIsUpWord("IN");
      case Ast::OP_LIKE: return NextTokenIsUpWord("LIKE");
      case Ast::OP_OR: return NextTokenIsUpWord("OR");
      case Ast::OP_AND: return NextTokenIsUpWord("AND");
      case Ast::OP_NOT: return NextTokenIsUpWord("NOT");
      case Ast::OP_EQ: return NextTokenIs(Token::EQ);
      case Ast::OP_LT: return NextTokenIs(Token::LT);
      case Ast::OP_GT: return NextTokenIs(Token::GT);
      case Ast::OP_LE: return NextTokenIs(Token::LE);
      case Ast::OP_GE: return NextTokenIs(Token::GE);
      case Ast::OP_NE: return NextTokenIs(Token::NE);
      case Ast::OP_PLUS: return NextTokenIs(Token::PLUS);
      case Ast::OP_MINUS: return NextTokenIs(Token::MINUS);
      case Ast::OP_BITWISE_AND: return NextTokenIs(Token::AMPERSAND);
      case Ast::OP_BITWISE_OR: return NextTokenIs(Token::PIPE);
      case Ast::OP_BITWISE_XOR: return NextTokenIs(Token::CARET);
      case Ast::OP_MUL: return NextTokenIs(Token::STAR);
      case Ast::OP_DIV: return NextTokenIs(Token::SLASH);
      case Ast::OP_MOD: return NextTokenIs(Token::PERCENT);
      case Ast::OP_BITWISE_NOT: return NextTokenIs(Token::TILDE);
      default: LOG(FATAL) << "NextTokenIsOp() called with a non-OP";
    }
  }

  inline bool MaybeConsumeToken(Token::Type type) {
    if (NextTokenIs(type)) {
      ++i;
      return true;
    }
    return false;
  }
};

// Returns an error at the most recently processed token.
Status Err(const Parser *p, string_view msg) {
  if (!p->i)
    return InvalidArgumentError(StrCat("Error at the start of input: ", msg));
  if (p->i - 1 >= p->tokens.size())
    return InvalidArgumentError(StrCat("Internal error in SQL parser: ", msg));
  return InvalidArgumentError(StrCat(
      "Error at ", p->LastToken().GetPositionIn(p->sql), ": ", msg));
}

// Parses the next token and expects it to be of the given type.
Status ParseToken(Token::Type type, Parser *p) {
  const Token expected = {type, p->CurrOffset(), 1};
  if (p->i >= p->tokens.size())
    return InvalidArgumentError(StrCat("Expected ", expected.ToString()));
  if (p->tokens[p->i++].type != type)
    return Err(p, StrCat("Expected ", expected.ToString(),
                         ", got ", p->LastTokenStr()));
  return OkStatus();
}

StatusOr<std::unique_ptr<Ast>> MaybeParseIfExistsStatement(
    std::unique_ptr<Ast> &&ast, Parser *p) {
  // Check if there are additional tokens to parse
  if (p->i < p->tokens.size() && p->tokens[p->i].type == Token::WORD &&
      p->tokens[p->i].word == "IF") {
    ++p->i;
    // Parse if expression: condition will go to lhs, body to rhs
    bool negate = false;
    if (p->NextTokenIsUpWord("NOT")) {
      ++p->i;
      negate = true;
    }

    if (!p->NextTokenIsUpWord("EXISTS")) {
      return InvalidArgumentError("Expected keyword EXISTS");
    }
    ++p->i;

    ::absl::string_view table_name = ast->table_name();
    ::absl::string_view index_name = ast->index_name();

    return StatusOr<std::unique_ptr<Ast>>(Ast::CreateConditionalStatement(
        std::move(
            Ast::CreateObjectExistsStatement(table_name, index_name, negate)),
        std::move(ast)));
  } else {
    return StatusOr<std::unique_ptr<Ast>>(std::move(ast));
  }
}

// Parses a semicolon and returns the given Ast.
StatusOr<std::unique_ptr<Ast>> ParseSemicolon(
    std::unique_ptr<Ast> ast, Parser *p) {
  Status s = ParseToken(Token::SEMICOLON, p);
  if (!s.ok()) return s;
  return ast;
}

// Parses the given uppercase keyword, ignoring the case in the input.
Status ParseKeyword(string_view upword, Parser *p) {
  Status s = ParseToken(Token::WORD, p);
  if (!s.ok()) return InvalidArgumentError(StrCat("Expected ", upword));
  const std::string w = AsciiStrToUpper(p->LastToken().word);
  if (upword != w) return InvalidArgumentError(StrCat(
      "Expected ", upword, ", got ", w));
  return OkStatus();
}

StatusOr<std::string> ParseTableName(Parser *p) {
  if (p->i >= p->tokens.size())
    return InvalidArgumentError("Expected table name");
  const Token &t = p->tokens[p->i++];
  if (t.type != Token::WORD)
    return Err(p, StrCat("Expected table name, got ", t.ToString()));
  if (t.word.empty()) return Err(p, "Empty table name");
  if (!isalpha(t.word[0]) && t.word[0] != '_')
    return Err(p, StrCat("Invalid table name: ", t.word));
  for (char c : t.word) {
    if (!isalpha(c) && !isdigit(c) && c != '_')
      return Err(p, StrCat("Invalid table name: ", t.word));
  }
  return t.word;
}

StatusOr<std::string> ParseColumnName(Parser *p) {
  if (p->i >= p->tokens.size())
    return InvalidArgumentError("Expected column name");
  const Token &t = p->tokens[p->i++];
  if (t.type != Token::WORD)
    return Err(p, StrCat("Expected column name, got ", t.ToString()));
  if (t.word.empty()) return Err(p, "Empty column name");
  if (!isalpha(t.word[0]) && t.word[0] != '_')
    return Err(p, StrCat("Invalid column name: ", t.word));
  for (char c : t.word) {
    if (!isalpha(c) && !isdigit(c) && c != '_')
      return Err(p, StrCat("Invalid column name: ", t.word));
  }
  return t.word;
}

// Parses the full name of a proto message type or proto enum type.
StatusOr<std::string> ParseFullProtoName(Parser *p) {
std::string name;
  do {
    if (!p->MaybeConsumeToken(Token::WORD))
      return Err(p, "Expected proto package or message name");
    if (!name.empty()) name += ".";
    name += p->LastToken().word;
    // TODO: disallow whitespace before the next DOT.
  } while (p->MaybeConsumeToken(Token::DOT));
  return name;
}

StatusOr<std::string> ParseColumnType(Parser *p) {
  if (p->i >= p->tokens.size())
    return InvalidArgumentError("Expected column type");
  const Token &t = p->tokens[p->i++];
  if (t.type != Token::WORD)
    return Err(p, StrCat("Expected column type, got ", t.ToString()));
  if (t.word.empty()) return Err(p, "Empty column type");

std::string type = AsciiStrToLower(t.word);
  if (t.word == "Proto" || t.word == "Enum") {
    if (!p->MaybeConsumeToken(Token::LT))
      return Err(p, StrCat("Expected < after ", t.word));
    StatusOr<std::string> so = ParseFullProtoName(p);
    if (!so.ok()) return so.status();
    if (!p->MaybeConsumeToken(Token::GT))
      return Err(p, StrCat("Expected > after ", so.ValueOrDie()));
    type = StrCat(t.word, "<", so.ValueOrDie(), ">");
  }

  if (p->MaybeConsumeToken(Token::BRACKET_OPEN)) {
    if (!p->MaybeConsumeToken(Token::BRACKET_CLOSE))
      return Err(p, "Expected ] after [");
    type += "[]";
  }

  return type;
}

// pre-declarations for recursion
StatusOr<std::unique_ptr<Ast>> ParseExpression(Parser *p);
StatusOr<std::unique_ptr<Ast>> ParseExpressionAtPrecedence(
    uint64 precedences_row, Parser *p);

StatusOr<std::unique_ptr<Ast>> ParseValue(Parser *p) {
  if (p->NextTokenIs(Token::PAREN_OPEN)) {
    // Parenthesized sub-expression.
    p->i++;
    StatusOr<std::unique_ptr<Ast>> so = ParseExpression(p);
    if (!so.ok()) return so;
    Status s = ParseToken(Token::PAREN_CLOSE, p);
    if (!s.ok()) return s;
    return std::move(so.ValueOrDie());
  }
  if (p->NextTokenIsUpWord("NOT")) {
    // Unary NOT. It has priority 1.5.
    StatusOr<std::unique_ptr<Ast>> so = ParseExpressionAtPrecedence(2, p);
    if (!so.ok()) return so;
    return Ast::UnaryOp(Ast::OP_NOT, std::move(so.ValueOrDie()));
  }
  if (p->NextTokenIs(Token::MINUS) || p->NextTokenIs(Token::TILDE)) {
    // Another unary operator. These have priority -0.5.
    Ast::Type op = (p->tokens[p->i++].type == Token::MINUS) ?
        Ast::OP_MINUS : Ast::OP_BITWISE_NOT;
    StatusOr<std::unique_ptr<Ast>> so = ParseValue(p);
    if (!so.ok()) return so;
    return Ast::UnaryOp(op, std::move(so.ValueOrDie()));
  }
  if (p->NextTokenIs(Token::INT64)) {
    // Integer.
    return Ast::Int64(p->tokens[p->i++].i64);
  }
  if (p->NextTokenIs(Token::DOUBLE)) {
    // Double.
    return Ast::Double(p->tokens[p->i++].dbl);
  }
  if (p->NextTokenIs(Token::QUOTED_STRING)) {
    // Quoted string.
    return Ast::QuotedString(p->tokens[p->i++].str);
  }
  if (p->NextTokenIs(Token::WORD)) {
    // Variable or function call.
    const std::string &var = p->tokens[p->i++].word;
    if (!p->MaybeConsumeToken(Token::PAREN_OPEN)) return Ast::Var(var);
    std::vector<std::unique_ptr<Ast>> values;
    while (!p->MaybeConsumeToken(Token::PAREN_CLOSE)) {
      if (!values.empty()) {
        const Status s = ParseToken(Token::COMMA, p);
        if (!s.ok()) return s;
      }
      if (p->MaybeConsumeToken(Token::PAREN_CLOSE)) break;
      StatusOr<std::unique_ptr<Ast>> so = ParseExpression(p);
      if (!so.ok()) return so.status();
      values.push_back(std::move(so.ValueOrDie()));
    }
    return Ast::Func(var, std::move(values));
  }

  if (p->i < p->tokens.size()) p->i++;
  return Err(p, "Expected a value");
}

StatusOr<std::unique_ptr<Ast>> ParseExpressionAtPrecedence(
    uint64 precedences_row, Parser *p) {
  // Binary operators in rows of increasing precedence,
  // with each row terminated by an ERROR sentinel.
  static const Ast::Type precedences[6][7] = {
    {Ast::OP_IN, Ast::OP_LIKE, Ast::OP_OR, Ast::ERROR},
    {Ast::OP_AND, Ast::ERROR},
    {Ast::OP_EQ, Ast::OP_LT, Ast::OP_GT, Ast::OP_LE, Ast::OP_GE, Ast::OP_NE,
        Ast::ERROR},
    {Ast::OP_PLUS, Ast::OP_MINUS, Ast::OP_BITWISE_AND, Ast::OP_BITWISE_OR,
        Ast::OP_BITWISE_XOR, Ast::ERROR},
    {Ast::OP_MUL, Ast::OP_DIV, Ast::OP_MOD, Ast::ERROR},
  };
  if (precedences_row == 5) return ParseValue(p);

  // left-hand side
  StatusOr<std::unique_ptr<Ast>> lhs_so =
      ParseExpressionAtPrecedence(precedences_row + 1, p);
  if (!lhs_so.ok()) return lhs_so;
  std::unique_ptr<Ast> lhs = std::move(lhs_so.ValueOrDie());

  while (true) {
    // operator
    const Ast::Type *op = precedences[precedences_row];
    while (*op != Ast::ERROR && !p->NextTokenIsOp(*op)) ++op;
    if (*op == Ast::ERROR) break;
    p->i++;

    // right-hand side
    StatusOr<std::unique_ptr<Ast>> rhs_so =
        ParseExpressionAtPrecedence(precedences_row + 1, p);
    if (!rhs_so.ok()) return rhs_so;
    std::unique_ptr<Ast> rhs = std::move(rhs_so.ValueOrDie());

    lhs = Ast::BinaryOp(*op, std::move(lhs), std::move(rhs));
  }

  return lhs;
}

StatusOr<std::unique_ptr<Ast>> ParseExpression(Parser *p) {
  return ParseExpressionAtPrecedence(0, p);
}

StatusOr<std::unique_ptr<Ast>> ParseCreateTable(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string table = so.ValueOrDie();

  const Status s = ParseToken(Token::PAREN_OPEN, p);
  if (!s.ok()) return s;

  std::vector<std::string> columns, column_types;
  while (true) {
    // Invariant: columns.empty() or we have just parsed a column type.
    if (p->i >= p->tokens.size()) return InvalidArgumentError("Unterminated (");
    if (p->tokens[p->i].type == Token::PAREN_CLOSE) {
      p->i++;
      break;
    }
    if (!columns.empty()) {
      const Status s = ParseToken(Token::COMMA, p);
      if (!s.ok()) return s;
    }
    if (p->i < p->tokens.size() && p->tokens[p->i].type == Token::PAREN_CLOSE) {
      // optonal trailing comma case
      p->i++;
      break;
    }

    const StatusOr<std::string> so2 = ParseColumnName(p);
    if (!so2.ok()) return so2.status();

    const StatusOr<std::string> so3 = ParseColumnType(p);
    if (!so3.ok()) return so3.status();

    columns.push_back(so2.ValueOrDie());
    column_types.push_back(so3.ValueOrDie());
  }

  std::unique_ptr<Ast> ast =
      Ast::CreateTable(table, std::move(columns), std::move(column_types));

  auto stmt_with_if =
      MaybeParseIfExistsStatement(std::move(ast), p).ValueOrDie();
  return ParseSemicolon(std::move(stmt_with_if), p);
}

StatusOr<std::unique_ptr<Ast>> ParseDropTable(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string table = so.ValueOrDie();

  std::unique_ptr<Ast> ast = Ast::DropTable(table);

  auto stmt_with_if =
      MaybeParseIfExistsStatement(std::move(ast), p).ValueOrDie();
  return ParseSemicolon(std::move(stmt_with_if), p);
}

StatusOr<std::unique_ptr<Ast>> ParseCreateIndex(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string index = so.ValueOrDie();

  const Status s = ParseKeyword("ON", p);
  if (!s.ok()) return s;

  const StatusOr<std::string> so2 = ParseTableName(p);
  if (!so2.ok()) return so2.status();
  const std::string table = so2.ValueOrDie();

  const Status s2 = ParseToken(Token::PAREN_OPEN, p);
  if (!s2.ok()) return s2;

  std::vector<std::string> columns;
  while (!p->MaybeConsumeToken(Token::PAREN_CLOSE)) {
    if (!columns.empty()) {
      Status s3 = ParseToken(Token::COMMA, p);
      if (!s3.ok()) return s3;
    }
    if (p->MaybeConsumeToken(Token::PAREN_CLOSE)) break;

    const StatusOr<std::string> so3 = ParseColumnName(p);
    if (!so3.ok()) return so3.status();
    columns.push_back(so3.ValueOrDie());
  }

  if (columns.empty()) return Err(p, "At least one column is required");

  std::unique_ptr<Ast> ast = Ast::CreateIndex(table, std::move(columns), index);
  auto stmt_with_if =
      MaybeParseIfExistsStatement(std::move(ast), p).ValueOrDie();
  return ParseSemicolon(std::move(stmt_with_if), p);
}

StatusOr<std::unique_ptr<Ast>> ParseDropIndex(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string index = so.ValueOrDie();

  std::unique_ptr<Ast> ast = Ast::DropIndex(index);
  auto stmt_with_if =
      MaybeParseIfExistsStatement(std::move(ast), p).ValueOrDie();
  return ParseSemicolon(std::move(stmt_with_if), p);
}

StatusOr<std::unique_ptr<Ast>> ParseCreate(Parser *p) {
  if (p->i >= p->tokens.size()) return InvalidArgumentError("CREATE what?");
  if (p->tokens[p->i].type == Token::WORD) {
    const std::string up_word = AsciiStrToUpper(p->tokens[p->i++].word);
    if (up_word == "TABLE") return ParseCreateTable(p);
    if (up_word == "INDEX") return ParseCreateIndex(p);
  }
  return Err(p, StrCat("Unexpected ", p->LastTokenStr()));
}

StatusOr<std::unique_ptr<Ast>> ParseDrop(Parser *p) {
  if (p->i >= p->tokens.size()) return InvalidArgumentError("DROP what?");
  if (p->tokens[p->i].type == Token::WORD) {
    const std::string up_word = AsciiStrToUpper(p->tokens[p->i++].word);
    if (up_word == "TABLE") return ParseDropTable(p);
    if (up_word == "INDEX") return ParseDropIndex(p);
  }
  return Err(p, StrCat("Unexpected ", p->LastTokenStr()));
}

StatusOr<std::unique_ptr<Ast>> ParseInsert(Parser *p) {
  const Status s = ParseKeyword("INTO", p);
  if (!s.ok()) return s;

  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string table = so.ValueOrDie();

  const Status s2 = ParseToken(Token::PAREN_OPEN, p);
  if (!s2.ok()) return s2;

  std::vector<std::string> columns;
  while (!p->MaybeConsumeToken(Token::PAREN_CLOSE)) {
    if (!columns.empty()) {
      const Status s3 = ParseToken(Token::COMMA, p);
      if (!s3.ok()) return s3;
    }
    if (p->MaybeConsumeToken(Token::PAREN_CLOSE)) break;

    const StatusOr<std::string> so2 = ParseColumnName(p);
    if (!so2.ok()) return so2.status();

    columns.push_back(so2.ValueOrDie());
  }

  const Status s4 = ParseKeyword("VALUES", p);
  if (!s4.ok()) return s4;

  const Status s5 = ParseToken(Token::PAREN_OPEN, p);
  if (!s5.ok()) return s5;

  std::vector<std::unique_ptr<Ast>> values;
  while (!p->MaybeConsumeToken(Token::PAREN_CLOSE)) {
    if (!values.empty()) {
      const Status s6 = ParseToken(Token::COMMA, p);
      if (!s6.ok()) return s6;
    }
    if (p->MaybeConsumeToken(Token::PAREN_CLOSE)) break;

    StatusOr<std::unique_ptr<Ast>> so3 = ParseExpression(p);
    if (!so3.ok()) return so3.status();

    values.push_back(std::move(so3.ValueOrDie()));
  }

  if (values.size() != columns.size()) return Err(p, StrCat(
      values.size(), " values given for ", columns.size(), " columns"));

  std::unique_ptr<Ast> ast = Ast::Insert(
      table, std::move(columns), std::move(values));
  return ParseSemicolon(std::move(ast), p);
}

StatusOr<std::unique_ptr<Ast>> ParseSelect(Parser *p, Token::Type terminal);

StatusOr<std::unique_ptr<Ast>> ParseFromClause(Parser *p) {
  if (p->NextTokenIs(Token::PAREN_OPEN)) {
    p->i++;
    if (!p->NextTokenIsUpWord("SELECT")) {
      if (p->i + 1 < p->tokens.size()) p->i++;
      return Err(p, "Expected SELECT after (");
    }
    p->i++;
    return ParseSelect(p, Token::PAREN_CLOSE);
  }
  if (p->NextTokenIs(Token::WORD)) {
    return Ast::TableScan(p->tokens[p->i++].word);
  }
  if (p->i + 1 < p->tokens.size()) p->i++;
  return Err(p, "Expected table name or sub-query");
}

// TODO: this function may have issues with numeric overflow
//       so it might need refactoring.
StatusOr<int32> ParseGroupByField(
    Parser *p, const std::vector<std::string> &columns) {
  if (p->NextTokenIs(Token::INT64) &&
      p->tokens[p->i].i64 > 0) {
    const int32 v = p->tokens[p->i++].i64;
    if (v < 1 || static_cast<size_t>(v) > columns.size())
      return Err(p, StrCat(
        "Column index ", v, " must be >=1 and <=", columns.size()));
    return v - 1;
  }
  if (p->NextTokenIs(Token::WORD)) {
    const std::string &w = p->tokens[p->i++].word;
    for (uint32 i = 0; i < columns.size(); ++i)
      if (columns[i] == w) return i;
    return Err(p, StrCat(w, " is not a named output column"));
  }
  if (p->i + 1 < p->tokens.size()) p->i++;
  return Err(p, "Expected column name or integer");
}

// Returns a column index, bitwise negated if the order is descending.
StatusOr<int32> ParseOrderByField(
    Parser *p, const std::vector<std::string> &columns) {
  StatusOr<int32> so = ParseGroupByField(p, columns);
  if (!so.ok()) return so.status();

  if (p->NextTokenIsUpWord("DESC")) {
    p->i++;
    return ~so.ValueOrDie();
  }
  if (p->NextTokenIsUpWord("ASC")) p->i++;
  return so.ValueOrDie();
}

StatusOr<std::unique_ptr<Ast>> ParseSelect(
    Parser *p, Token::Type terminal = Token::SEMICOLON) {
  std::vector<std::string> columns;
  std::vector<std::unique_ptr<Ast>> values;

  do {
    StatusOr<std::unique_ptr<Ast>> so = ParseExpression(p);
    if (!so.ok()) return so.status();
    values.push_back(std::move(so.ValueOrDie()));

    if (p->NextTokenIsUpWord("AS")) {
      p->i++;
      StatusOr<std::string> so2 = ParseColumnName(p);
      if (!so2.ok()) return so2.status();
      columns.push_back(so2.ValueOrDie());
    } else {
      columns.push_back("");
    }
  } while (p->MaybeConsumeToken(Token::COMMA));
  CHECK_EQ(columns.size(), values.size()) << "ParseSelect bug";

  std::unique_ptr<Ast> from = nullptr;
  if (p->NextTokenIsUpWord("FROM")) {
    p->i++;
    StatusOr<std::unique_ptr<Ast>> so3 = ParseFromClause(p);
    if (!so3.ok()) return so3.status();
    from = std::move(so3.ValueOrDie());
  }

  std::unique_ptr<Ast> where = nullptr;
  if (p->NextTokenIsUpWord("WHERE")) {
    p->i++;
    if (!from) return Err(p, "Unexpected WHERE without FROM");
    StatusOr<std::unique_ptr<Ast>> so4 = ParseExpression(p);
    if (!so4.ok()) return so4.status();
    where = std::move(so4.ValueOrDie());
  }

  std::vector<int32> group_by;
  if (p->NextTokenIsUpWord("GROUP")) {
    p->i++;
    if (!from) return Err(p, "Unexpected GROUP without FROM");
    if (!p->NextTokenIsUpWord("BY")) {
      if (p->i + 1 < p->tokens.size()) p->i++;
      return Err(p, "Expected BY after GROUP");
    }
    p->i++;
    do {
      StatusOr<int32> so5 = ParseGroupByField(p, columns);
      if (!so5.ok()) return so5.status();
      group_by.push_back(so5.ValueOrDie());
    } while (p->MaybeConsumeToken(Token::COMMA));
  }

  std::vector<int32> order_by;
  if (p->NextTokenIsUpWord("ORDER")) {
    p->i++;
    if (!from) return Err(p, "Unexpected ORDER without FROM");
    if (!p->NextTokenIsUpWord("BY")) {
      if (p->i + 1 < p->tokens.size()) p->i++;
      return Err(p, "Expected BY after ORDER");
    }
    p->i++;
    do {
      StatusOr<int32> so6 = ParseOrderByField(p, columns);
      if (!so6.ok()) return so6.status();
      order_by.push_back(so6.ValueOrDie());
    } while (p->MaybeConsumeToken(Token::COMMA));
  }

  Status s = ParseToken(terminal, p);
  if (!s.ok()) return s;

  std::unique_ptr<Ast> ast = from ? std::move(from) : Ast::SingleEmptyRow();
  if (where)
    ast = Ast::Filter(std::move(where), std::move(ast));
  ast = Ast::Map(std::move(columns), std::move(values), std::move(ast));
  if (!group_by.empty())
    ast = Ast::GroupBy(std::move(ast), std::move(group_by));
  if (!order_by.empty())
    ast = Ast::OrderBy(std::move(ast), std::move(order_by));
  return ast;
}

StatusOr<std::unique_ptr<Ast>> ParseUpdate(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();
  const std::string table = so.ValueOrDie();

  const Status s = ParseKeyword("SET", p);
  if (!s.ok()) return s;

  std::vector<std::string> columns;
  std::vector<std::unique_ptr<Ast>> values;
  do {
    const StatusOr<std::string> so2 = ParseColumnName(p);
    if (!so2.ok()) return so2.status();

    const Status s2 = ParseToken(Token::EQ, p);
    if (!s2.ok()) return s2;

    StatusOr<std::unique_ptr<Ast>> so3 = ParseExpression(p);
    if (!so3.ok()) return so3.status();

    columns.push_back(so2.ValueOrDie());
    values.push_back(std::move(so3.ValueOrDie()));
  } while (p->MaybeConsumeToken(Token::COMMA));

  const Status s2 = ParseKeyword("WHERE", p);
  if (!s2.ok()) return s2;

  StatusOr<std::unique_ptr<Ast>> so4 = ParseExpression(p);
  if (!so4.ok()) return so4.status();
  return Ast::Update(
      table, std::move(columns), std::move(values),
      std::move(so4.ValueOrDie()));
}

StatusOr<std::unique_ptr<Ast>> ParseShowTables(Parser *p) {
  const Status s = ParseKeyword("TABLES", p);
  if (!s.ok()) return s;
  return Ast::ShowTables();
}

StatusOr<std::unique_ptr<Ast>> ParseDescribeTable(Parser *p) {
  const StatusOr<std::string> so = ParseTableName(p);
  if (!so.ok()) return so.status();

  return Ast::DescribeTable(so.ValueOrDie());
}

StatusOr<std::unique_ptr<Ast>> Parse(Parser *p) {
  if (p->i >= p->tokens.size()) return InvalidArgumentError("Empty statement");
  if (p->tokens[p->i].type == Token::WORD) {
    const std::string up_word = AsciiStrToUpper(p->tokens[p->i++].word);
    if (up_word == "CREATE") return ParseCreate(p);
    if (up_word == "DROP") return ParseDrop(p);
    if (up_word == "INSERT") return ParseInsert(p);
    if (up_word == "SELECT") return ParseSelect(p);
    if (up_word == "UPDATE") return ParseUpdate(p);
    if (up_word == "SHOW") return ParseShowTables(p);
    if (up_word == "DESCRIBE") return ParseDescribeTable(p);
  }
  return Err(p, StrCat("Unexpected ", p->LastTokenStr()));
}

}  // namespace

StatusOr<std::unique_ptr<Ast>> Parse(string_view sql) {
  StatusOr<std::vector<Token>> so = Tokenize(sql);
  if (!so.ok()) return so.status();
  Parser p = {sql, so.ValueOrDie(), 0};
  return Parse(&p);
}

}  // namespace sfdb
