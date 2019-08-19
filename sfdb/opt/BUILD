# SQL optimizer.
#
# Converts a TypedAst into a more efficient TypedAst.

package(default_visibility = ["//visibility:private"])

# ------------------------------------------------------------------------------
# Public library
# ------------------------------------------------------------------------------

cc_library(
    name = "opt",
    srcs = ["opt.cc"],
    hdrs = ["opt.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":index_match",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "@com_google_absl//absl/memory",
    ],
)

# ------------------------------------------------------------------------------
# Private libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "index_match",
    srcs = ["index_match.cc"],
    hdrs = ["index_match.h"],
    deps = [
        "//sfdb/base:ast",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//sfdb/engine:set_field",
        "//util/task:status",
        "@com_google_absl//absl/memory",
        "@com_google_protobuf//:protobuf",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "opt_test",
    size = "small",
    srcs = ["opt_test.cc"],
    deps = [
        ":opt",
        "//sfdb/base:ast_type",
        "//sfdb/base:db",
        "//sfdb/base:typed_ast",
        "//sfdb/base:vars",
        "//sfdb/proto:pool",
        "//sfdb/sql:parser",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
