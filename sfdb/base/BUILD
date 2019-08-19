# Basic types and libraries used throughout the service.
package(default_visibility = ["//visibility:public"])

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "ast",
    srcs = ["ast.cc"],
    hdrs = ["ast.h"],
    deps = [
        ":value",
        "//util/types",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "db",
    srcs = ["db.cc"],
    hdrs = ["db.h"],
    deps = [
        ":vars",
        "//sfdb/proto:pool",
        "@com_github_glog_glog//:glog",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "ast_type",
    srcs = ["ast_type.cc"],
    hdrs = ["ast_type.h"],
    deps = [
        "//sfdb/proto:pool",
        "//util/task:status",
        "//util/task:statusor",
        "@com_github_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "funcs",
    srcs = ["funcs.cc"],
    hdrs = ["funcs.h"],
    deps = [
        "//sfdb/base:ast_type",
        "//sfdb/base:value",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "proto_stream",
    hdrs = ["proto_stream.h"],
    deps = [
        "//util/task:status",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "replicated_db",
    hdrs = ["replicated_db.h"],
    deps = [
        "//sfdb:api",
        "//util/task:status",
    ],
)

cc_library(
    name = "typed_ast",
    hdrs = ["typed_ast.h"],
    deps = [
        ":ast",
        ":ast_type",
        "//util/types",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "value",
    srcs = ["value.cc"],
    hdrs = ["value.h"],
    deps = [
        ":ast_type",
        "//sfdb/proto:dup_message",
        "//util/task:status",
        "//util/task:statusor",
        "//util/types",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "vars",
    srcs = ["vars.cc"],
    hdrs = ["vars.h"],
    deps = [
        ":ast_type",
        ":funcs",
        ":value",
        "//sfdb/proto:field_path",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "ast_type_test",
    size = "small",
    srcs = ["ast_type_test.cc"],
    deps = [
        ":ast_type",
        "//sfdb/proto:pool",
        "//sfdb/testing:data",
        "//util/task:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "db_test",
    size = "small",
    srcs = ["db_test.cc"],
    deps = [
        ":db",
        ":vars",
        "//sfdb/proto:pool",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "funcs_test",
    size = "small",
    srcs = ["funcs_test.cc"],
    deps = [
        ":funcs",
        "//sfdb/base:ast_type",
        "//sfdb/base:value",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "value_test",
    size = "small",
    srcs = ["value_test.cc"],
    deps = [
        ":value",
        "//sfdb/testing:data",
        "//util/proto",
        "//util/task:status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "vars_test",
    size = "small",
    srcs = ["vars_test.cc"],
    deps = [
        ":ast_type",
        ":value",
        ":vars",
        "//sfdb/testing:data",
        "//util/proto",
        "//util/task:status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
