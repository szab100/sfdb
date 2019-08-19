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

#include <string>
#include <vector>

#include "util/task/canonical_errors.h"
#include "util/task/statusor.h"
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace sfdb {
namespace {

using ::testing::HasSubstr;

typedef ::std::vector<Token> Tokens;

// Tokenizes and asserts success.
Tokens Go(const char *sql) {
  ::util::StatusOr<Tokens> s = Tokenize(sql);
  CHECK_OK(s.status()) << "sql = " << sql;
  return s.ValueOrDie();
}

// Tokenizes and asserts failure.
::std::string Err(const char *sql) {
  ::util::StatusOr<Tokens> s = Tokenize(sql);
  CHECK(!s.ok()) << "sql = " << sql;
  CHECK(IsInvalidArgument(s.status())) << s.status();
  return s.status().error_message();
}

TEST(TokenizerTest, Basic) {
  EXPECT_EQ(Tokens(), Go(""));
  EXPECT_EQ(Tokens(), Go(" \t\r\n"));

  const char *sql1 = "SELECT";
  EXPECT_EQ(Tokens({Token::Word(0, 6, sql1)}), Go(sql1));

  const char *sql2 = " SELECT\n";
  EXPECT_EQ(Tokens({Token::Word(1, 6, sql2)}), Go(sql2));

  const char *sql3 = "DROP TABLE";
  EXPECT_EQ(Tokens({
      Token::Word(0, 4, sql3),
      Token::Word(5, 5, sql3)}), Go(sql3));

  const char *sql4 = R"(SELECT * FROM MyTable WHERE sky.color = "blue";)";
  EXPECT_EQ(Tokens({
      Token::Word(0, 6, sql4),             // SELECT
      {Token::STAR, 7, 1},                 // *
      Token::Word(9, 4, sql4),             // FROM
      Token::Word(14, 7, sql4),            // MyTable
      Token::Word(22, 5, sql4),            // WHERE
      Token::Word(28, 3, sql4),            // sky
      {Token::DOT, 31, 1},                 // .
      Token::Word(32, 5, sql4),            // color
      {Token::EQ, 38, 1},                  // =
      Token::QuotedString(40, 6, "blue"),  // "blue"
      {Token::SEMICOLON, 46, 1}            // ;
      }), Go(sql4));

  const char *sql5 = "CREATE TABLE Mixed_case (name string, age int64);";
  EXPECT_EQ(Tokens({
      Token::Word(0, 6, sql5),      // CREATE
      Token::Word(7, 5, sql5),      // TABLE
      Token::Word(13, 10, sql5),    // Mixed_case
      {Token::PAREN_OPEN, 24, 1},   // (
      Token::Word(25, 4, sql5),     // name
      Token::Word(30, 6, sql5),     // string
      {Token::COMMA, 36, 1},        // ,
      Token::Word(38, 3, sql5),     // age
      Token::Word(42, 5, sql5),     // int64
      {Token::PAREN_CLOSE, 47, 1},  // )
      {Token::SEMICOLON, 48, 1}     // ;
      }), Go(sql5));
}

TEST(TokenizerTest, Numbers) {
  // decimal integers
  EXPECT_EQ(Tokens({Token::Int64(0, 1, 0)}), Go("0"));
  EXPECT_EQ(Tokens({Token::Int64(0, 1, 1)}), Go("1"));
  EXPECT_EQ(Tokens({Token::Int64(0, 2, 13)}), Go("13"));
  EXPECT_EQ(Tokens({Token::Int64(0, 10, 1234567890)}), Go("1234567890"));
  EXPECT_EQ(Tokens({Token::Int64(0, 19, 0x7fffffffffffffff)}),
            Go("9223372036854775807"));

  // hex
  EXPECT_EQ(Tokens({Token::Int64(0, 3, 0)}), Go("0x0"));
  EXPECT_EQ(Tokens({Token::Int64(0, 10, 0xdeadbeef)}), Go("0xdeadBEEF"));
  EXPECT_EQ(Tokens({Token::Int64(0, 18, 0x0123456789abcdef)}),
            Go("0x0123456789abcdef"));
  EXPECT_EQ(Tokens({Token::Int64(0, 18, 0x0123456789abcdef)}),
            Go("0x0123456789ABCDEF"));
  EXPECT_EQ(Tokens({Token::Int64(0, 18, -1)}),
            Go("0xFFFFFFFFffffffff"));
  EXPECT_EQ(Tokens({Token::Int64(0, 4, 0x13)}), Go("0x13 "));

  // octal
  EXPECT_EQ(Tokens({Token::Int64(0, 2, 0)}), Go("00"));
  EXPECT_EQ(Tokens({Token::Int64(0, 9, 001234567)}), Go("001234567"));

  // floats
  EXPECT_EQ(Tokens({Token::Double(0, 2, 0)}), Go("0."));
  EXPECT_EQ(Tokens({Token::Double(0, 3, 0)}), Go("0.0"));
  EXPECT_EQ(Tokens({Token::Double(0, 2, 1.)}), Go("1."));
  EXPECT_EQ(Tokens({Token::Double(0, 3, 1.0)}), Go("1.0"));
  EXPECT_EQ(Tokens({Token::Double(0, 11, 0.123456789)}), Go("0.123456789"));
  EXPECT_EQ(Tokens({Token::Double(0, 3, 1e9)}), Go("1e9"));
  EXPECT_EQ(Tokens({Token::Double(0, 6, 1.2e34)}), Go("1.2E34"));

  // junk
  EXPECT_THAT(Err("123eh45"), HasSubstr("Unexpected 'h'"));
  EXPECT_THAT(Err("9223372036854775808"), HasSubstr("overflow"));
  EXPECT_THAT(Err("1.2e5e6"), HasSubstr("Bad float"));
  EXPECT_THAT(Err("1.2."), HasSubstr("Bad float"));
  EXPECT_THAT(Err("04567832"), HasSubstr("Bad octal"));
  EXPECT_THAT(Err("0xxx"), HasSubstr("Unexpected 'x'"));
  EXPECT_THAT(Err("0xdaftpunk"), HasSubstr("Unexpected 't'"));
  EXPECT_THAT(Err("0xffffffffFFFFFFFFf"), HasSubstr("Bad hex int"));

  // termination
  EXPECT_EQ(Tokens({Token::Int64(0, 2, 99), {Token::PAREN_CLOSE, 2, 1}}),
            Go("99)"));
  EXPECT_EQ(
      Tokens({
          Token::Int64(0, 1, 1),
          Token::Int64(2, 1, 2),
          Token::Int64(4, 1, 3),
      }),
      Go("1 2 3"));
}

TEST(TokenizerTest, QuotedStrings) {
  EXPECT_EQ(
      Tokens({
          Token::QuotedString(0, 9, "cookies")
      }),
      Go(R"("cookies")"));
  EXPECT_EQ(
      Tokens({
          Token::QuotedString(0, 11, " \b\t\r\n")
      }),
      Go("' \\b\\t\\r\\n'"));
  EXPECT_EQ(
      Tokens({
          Token::QuotedString(0, 4, "\"")
      }),
      Go("\"\\\"\""));

  EXPECT_THAT(Err("'\n'"), HasSubstr("Invalid quoted character"));
}

TEST(TokenizerTest, Operators) {
  EXPECT_EQ(Tokens({{Token::LE, 0, 2}}), Go("<="));
  EXPECT_EQ(Tokens({{Token::GE, 0, 2}}), Go(">="));
  EXPECT_EQ(Tokens({{Token::NE, 0, 2}}), Go("<>"));

  const char *sql1 = "<=foo";
  EXPECT_EQ(Tokens({{Token::LE, 0, 2}, Token::Word(2, 3, sql1)}), Go(sql1));

  const char *sql2 = "a=b<c>d<=e>=f<>g";
  EXPECT_EQ(Tokens({
      Token::Word(0, 1, sql2),
      {Token::EQ, 1, 1},
      Token::Word(2, 1, sql2),
      {Token::LT, 3, 1},
      Token::Word(4, 1, sql2),
      {Token::GT, 5, 1},
      Token::Word(6, 1, sql2),
      {Token::LE, 7, 2},
      Token::Word(9, 1, sql2),
      {Token::GE, 10, 2},
      Token::Word(12, 1, sql2),
      {Token::NE, 13, 2},
      Token::Word(15, 1, sql2)}), Go(sql2));
}

TEST(TokenizerTest, Insert) {
  const char *sql = "INSERT INTO People (name, age) VALUES ('dude', 99);";
  EXPECT_EQ(Tokens({
      Token::Word(0, 6, sql),
      Token::Word(7, 4, sql),
      Token::Word(12, 6, sql),
      {Token::PAREN_OPEN, 19, 1},
      Token::Word(20, 4, sql),
      {Token::COMMA, 24, 1},
      Token::Word(26, 3, sql),
      {Token::PAREN_CLOSE, 29, 1},
      Token::Word(31, 6, sql),
      {Token::PAREN_OPEN, 38, 1},
      Token::QuotedString(39, 6, "dude"),
      {Token::COMMA, 45, 1},
      Token::Int64(47, 2, 99),
      {Token::PAREN_CLOSE, 49, 1},
      {Token::SEMICOLON, 50, 1}}), Go(sql));
}

TEST(TokenizerTest, Select) {
  const char *sql = "SELECT name, age FROM People WHERE age >= 21;";
  EXPECT_EQ(Tokens({
      Token::Word(0, 6, sql),
      Token::Word(7, 4, sql),
      {Token::COMMA, 11, 1},
      Token::Word(13, 3, sql),
      Token::Word(17, 4, sql),
      Token::Word(22, 6, sql),
      Token::Word(29, 5, sql),
      Token::Word(35, 3, sql),
      {Token::GE, 39, 2},
      Token::Int64(42, 2, 21),
      {Token::SEMICOLON, 44, 1}}), Go(sql));
}

TEST(TokenizeTest, Update) {
  const char *sql = "UPDATE People SET age=69, id=0x13 WHERE name = 'dude';";
  EXPECT_EQ(Tokens({
      Token::Word(0, 6, sql),             // UPDATE
      Token::Word(7, 6, sql),             // People
      Token::Word(14, 3, sql),            // SET
      Token::Word(18, 3, sql),            // age
      {Token::EQ, 21, 1},                 // =
      Token::Int64(22, 2, 69),            // 69
      {Token::COMMA, 24, 1},              // ,
      Token::Word(26, 2, sql),            // id
      {Token::EQ, 28, 1},                 // =
      Token::Int64(29, 4, 0x13),          // 0x13
      Token::Word(34, 5, sql),            // WHERE
      Token::Word(40, 4, sql),            // name
      {Token::EQ, 45, 1},                 // =
      Token::QuotedString(47, 6, "dude"), // 'dude'
      {Token::SEMICOLON, 53, 1}           // ;
      }), Go(sql));
}

}  // namespace
}  // namespace sfdb

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}