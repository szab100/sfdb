# Protobuf utils.

package(default_visibility = ["//visibility:public"])

load("@com_google_protobuf//:protobuf.bzl", "cc_proto_library")

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "dup_message",
    srcs = ["dup_message.cc"],
    hdrs = ["dup_message.h"],
    deps = [
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "field_path",
    srcs = ["field_path.cc"],
    hdrs = ["field_path.h"],
    deps = [
        ":dup_message",
        ":pool",
        "//sfdb/base:value",
        "//util/task:status",
        "//util/task:statusor",
        "//util/types",
        "@com_github_google_re2//:re2",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "pool",
    srcs = ["pool.cc"],
    hdrs = ["pool.h"],
    deps = [
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "field_path_test",
    size = "small",
    srcs = ["field_path_test.cc"],
    deps = [
        ":field_path",
        "//sfdb/base:value",
        "//sfdb/testing:data",
        "//util/proto",
        "//util/task:status",
        "@com_github_glog_glog//:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "pool_test",
    size = "small",
    srcs = ["pool_test.cc"],
    deps = [
        ":pool",
        "//util/task:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
