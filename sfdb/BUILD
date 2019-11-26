load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

# Small, fast database.
#
# An experiment in writing a mostly in-memory database.
#
# Uses RAFT for replication.

package(default_visibility = ["//visibility:public"])

# gRPC
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

# ------------------------------------------------------------------------------
# Binaries and MPMs
# ------------------------------------------------------------------------------

cc_binary(
    name = "sfdb",
    srcs = ["main.cc"],
    deps = [
        ":flags",
        "//server:server",
        "//server:grpc_sfdb_server",
        "//server:brpc_sfdb_server",
        "//util/thread",
        "@com_github_google_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

# ------------------------------------------------------------------------------
# Protos
# ------------------------------------------------------------------------------

proto_library(
    name = "api_proto",
    srcs = ["api.proto"],
    deps = [
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

cc_proto_library(
    name = "api",
    deps = [":api_proto"],
)

cc_grpc_library(
    name = "api_cc_grpc",
    srcs = [":api_proto"],
    grpc_only = True,
    deps = [":api"],
)

go_proto_library(
    name = "api_go_proto",
    compiler = "@io_bazel_rules_go//proto:go_grpc",
    importpath = "github.com/googlegsa/sfdb/api_go_proto",
    proto = ":api_proto",
    visibility = ["//visibility:public"],
)

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "flags",
    srcs = ["flags.cc"],
    hdrs = ["flags.h"],
    deps = [
        "//util/types",
        "@com_google_absl//absl/flags:flag",
    ],
)