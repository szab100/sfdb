# The RAFT module.
#
# Knows how to create and manage RAFT members.

package(default_visibility = ["//visibility:public"])

# ------------------------------------------------------------------------------
# Protos
# ------------------------------------------------------------------------------

proto_library(
    name = "mutation_proto",
    srcs = ["mutation.proto"],
)

cc_proto_library(
    name = "mutation",
    deps = [":mutation_proto"],
)

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "raft",
    srcs = ["raft_module.cc"],
    hdrs = ["raft_module.h"],
    deps = [
        ":instance",
        "//sfdb/base:db",
        "//util/time:clock",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
    ],
)

cc_library(
    name = "instance",
    srcs = ["instance.cc"],
    hdrs = ["instance.h"],
    deps = [
        ":mutation",
        "//raft",
        "//sfdb:api_cc_grpc",
        "//sfdb:flags",
        "//sfdb/base:ast",
        "//sfdb/base:db",
        "//sfdb/base:replicated_db",
        "//sfdb/base:typed_ast",
        "//sfdb/engine",
        "//sfdb/sql:parser",
        "//util/task:status",
        "//util/task:statusor",
        "//util/time:clock",
        "@com_github_glog_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)
