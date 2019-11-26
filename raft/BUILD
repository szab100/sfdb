# An implementation of the RAFT consensus algorithm.
#
# The paper: https://raft.github.io/raft.pdf

package(default_visibility = ["//visibility:private"])

# ------------------------------------------------------------------------------
# Public library
# ------------------------------------------------------------------------------

load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

cc_library(
    name = "raft",
    srcs = ["raft.cc"],
    hdrs = ["raft.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":options",
        ":raft_proto_cc_grpc",
        ":service_impl",
        "//util/task:status",
        "//util/types",
        "@com_google_absl//absl/strings",
    ],
)

# ------------------------------------------------------------------------------
# Private implementation
# ------------------------------------------------------------------------------

cc_library(
    name = "alarm_thread",
    srcs = ["alarm_thread.cc"],
    hdrs = ["alarm_thread.h"],
    deps = [
        "//util/thread",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "cluster",
    srcs = ["cluster.cc"],
    hdrs = ["cluster.h"],
    deps = [
        ":options",
        ":raft_proto_cc_grpc",
        "//util/grpc:async_wrappers",
        "//util/types",
        "@com_github_google_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "msg_ids",
    srcs = ["msg_ids.cc"],
    hdrs = ["msg_ids.h"],
    deps = [
        "//util/time:clock",
        "//util/types",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "options",
    hdrs = ["options.h"],
    deps = [
        "//util/task:status",
        "//util/time:clock",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

proto_library(
    name = "raft_proto",
    srcs = ["raft.proto"],
)

cc_grpc_library(
    name = "raft_proto_cc_grpc",
    srcs = [":raft_proto"],
    generate_mocks = True,
    grpc_only = True,
    deps = [":raft_proto_cc"],
)

cc_proto_library(
    name = "raft_proto_cc",
    deps = [":raft_proto"],
)

cc_library(
    name = "service_impl",
    srcs = ["service_impl.cc"],
    hdrs = ["service_impl.h"],
    deps = [
        ":alarm_thread",
        ":cluster",
        ":msg_ids",
        ":options",
        ":raft_proto_cc_grpc",
        "//util/task:status",
        "//util/types",
        "//util/varz",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "cluster_test",
    size = "small",
    srcs = ["cluster_test.cc"],
    deps = [
        ":cluster",
        ":raft_proto_cc",
        "//util/grpc:async_stub_testing",
        "//util/proto",
        "//util/task:status",
        "//util/thread",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "msg_ids_test",
    size = "small",
    srcs = ["msg_ids_test.cc"],
    deps = [
        ":msg_ids",
        "//util/thread",
        "//util/types",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
