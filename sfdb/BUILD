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

# cc_binary(
#     name = "sfdb",
#     srcs = ["main.cc"],
#     deps = [
#         ":flags",
#         ":modules",
#         ":service_impl",
#         "//base",
#         "@com_github_grpc_grpc//:grpc++",
#         "@com_google_absl//absl/flags:flag",
#         "@com_google_absl//absl/strings",
#         "@com_google_absl//absl/time",
#         "//util/thread",
#     ],
# )

# ------------------------------------------------------------------------------
# Protos
# ------------------------------------------------------------------------------

proto_library(
    name = "api_proto",
    srcs = ["api.proto"],
)

cc_proto_library(
    name = "api",
    deps = [":api_proto"],
)

cc_grpc_library(
    name = "api_cc_grpc",
    srcs = [":api_proto"],
    deps = [":api"],
    grpc_only = True,
)

# go_proto_library(
#     name = "api_go_proto",
#     compiler = "@io_bazel_rules_go//proto:go_grpc",
#     importpath = "github.com/googlegsa/sfdb/api_go_proto",
#     proto = ":api_proto",
#     visibility = ["//visibility:public"],
# )

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

# cc_library(
#     name = "modules",
#     srcs = ["modules.cc"],
#     hdrs = ["modules.h"],
#     deps = [
#         ":flags",
#         "//sfdb/base:db",
#         "//sfdb/base:vars",
#         "//sfdb/raft",
#         "@com_github_grpc_grpc//:grpc++",
#         "@com_google_absl//absl/flags:flag",
#         "@com_google_absl//absl/memory",
#         "@com_google_absl//absl/strings",
#         "//util/time:clock",
#     ],
# )

# cc_library(
#     name = "service_impl",
#     srcs = ["service_impl.cc"],
#     hdrs = ["service_impl.h"],
#     deps = [
#         ":api_cc_grpc",
#         ":modules",
#         "//base",
#         "//sfdb/base:replicated_db",
#     ],
# )
