# SQL execution engine.
#
# Knows how to apply Ast objects to Db instances.

package(default_visibility = ["//visibility:public"])

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "create_and_drop",
    srcs = ["create_and_drop.cc"],
    hdrs = ["create_and_drop.h"],
    deps = [
        "//sfdb/base:ast_type",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//util/task:status",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "expressions",
    srcs = ["expressions.cc"],
    hdrs = ["expressions.h"],
    deps = [
        "//sfdb/base:typed_ast",
        "//sfdb/base:value",
        "//sfdb/base:vars",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "engine",
    srcs = ["engine.cc"],
    hdrs = ["engine.h"],
    deps = [
        ":create_and_drop",
        ":expressions",
        ":infer_result_types",
        ":insert",
        ":proto_streams",
        ":select",
        ":update",
        ":utils",
        "//sfdb/base:ast",
        "//sfdb/base:ast_type",
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//sfdb/base:typed_ast",
        "//sfdb/base:value",
        "//sfdb/opt",
        "//sfdb/proto:pool",
        "//util/task:status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "infer_result_types",
    srcs = ["infer_result_types.cc"],
    hdrs = ["infer_result_types.h"],
    deps = [
        "//sfdb/base:ast",
        "//sfdb/base:ast_type",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "insert",
    srcs = ["insert.cc"],
    hdrs = ["insert.h"],
    deps = [
        ":expressions",
        ":set_field",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "proto_streams",
    srcs = ["proto_streams.cc"],
    hdrs = ["proto_streams.h"],
    deps = [
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//util/task:status",
        "//util/task:statusor",
        "@com_github_glog_glog//:glog",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "select",
    srcs = ["select.cc"],
    hdrs = ["select.h"],
    deps = [
        ":expressions",
        ":proto_streams",
        ":set_field",
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//sfdb/base:typed_ast",
        "//sfdb/base:vars",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        ":expressions",
        ":proto_streams",
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//sfdb/base:typed_ast",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "set_field",
    srcs = ["set_field.cc"],
    hdrs = ["set_field.h"],
    deps = [
        "//sfdb/base:value",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "update",
    srcs = ["update.cc"],
    hdrs = ["update.h"],
    deps = [
        ":expressions",
        ":proto_streams",
        ":set_field",
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//sfdb/base:typed_ast",
        "//sfdb/base:vars",
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_github_glog_glog//:glog",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "engine_test",
    size = "small",
    srcs = ["engine_test.cc"],
    deps = [
        ":engine",
        "//sfdb/base:vars",
        "//sfdb/proto:pool",
        "//sfdb/sql:parser",
        "//util/task:status",
        "//util/task:status_matchers",
        "//util/task:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "infer_result_types_test",
    size = "small",
    srcs = ["infer_result_types_test.cc"],
    deps = [
        ":infer_result_types",
        "//sfdb/base:ast",
        "//sfdb/base:ast_type",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//sfdb/proto:pool",
        "//sfdb/sql:parser",
        "//sfdb/testing:data",
        "//util/task:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_streams_test",
    size = "small",
    srcs = ["proto_streams_test.cc"],
    deps = [
        ":proto_streams",
        "//sfdb/base:db",
        "//sfdb/base:proto_stream",
        "//sfdb/testing:data",
        "//util/proto",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
