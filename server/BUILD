package(default_visibility = ["//visibility:public"])
# gRPC
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

cc_library(
  name = "common_types",
  srcs = ["common_types.h"],
  hdrs = ["common_types.h"],
)

cc_library(
  name = "server",
  srcs = ["server.h"],
  hdrs = ["server.h"],
)

cc_library(
  name = "braft_state_machine_impl",
  srcs = ["braft_state_machine_impl.cc"],
  hdrs = ["braft_state_machine_impl.h"],
  deps = [
        ":common_types",
        "//sfdb:api",
        "//util/task:codes_cpp",
        "@com_github_brpc_braft//:braft",
        "@com_github_brpc_brpc//:butil",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
  ],
)

proto_library(
    name = "brpc_service_proto",
    srcs = ["brpc_service.proto"],
    deps = [
        "//sfdb:api_proto",
    ],
)

cc_proto_library(
    name = "brpc_service",
    deps = [":brpc_service_proto"],
)

cc_library(
  name = "brpc_sfdb_service_impl",
  srcs = ["brpc_sfdb_service_impl.cc"],
  hdrs = ["brpc_sfdb_service_impl.h"],
  deps = [
        ":braft_node",
        ":brpc_service",
        "//sfdb:api",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
  ],
)

cc_library(
  name = "braft_node",
  srcs = ["braft_node.cc"],
  hdrs = ["braft_node.h"],
  deps = [
        ":common_types",
        ":braft_state_machine_impl",
        "//sfdb:api",
        "//util/task:codes_cpp",
        "//util/thread",
        "@com_github_brpc_braft//:braft",
        "@com_github_brpc_brpc//:butil",
        "@com_github_brpc_brpc//:brpc",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
  ],
)

cc_library(
  name = "brpc_sfdb_server",
  srcs = ["brpc_sfdb_server_impl.h", "brpc_sfdb_server_impl.cc", "brpc_sfdb_server.h", "brpc_sfdb_server.cc"],
  hdrs = ["brpc_sfdb_server.h"],
  deps = [
        ":common_types",
        ":braft_node",
        ":brpc_sfdb_service_impl",
        ":server",
        "//sfdb:api",
        "//sfdb/proto:pool",
        "//sfdb/sql:parser",
        "//sfdb/base:db",
        "//sfdb/base:ast",
        "//sfdb/engine:engine",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/memory",
  ],
)

proto_library(
    name = "grpc_sfdb_service_proto",
    srcs = ["grpc_service.proto"],
    deps = [
        "//sfdb:api_proto",
    ],
)

cc_proto_library(
    name = "grpc_sfdb_service_cc",
    deps = [":grpc_sfdb_service_proto"],
)

cc_grpc_library(
    name = "grpc_sfdb_service_cc_grpc",
    srcs = [":grpc_sfdb_service_proto"],
    grpc_only = True,
    deps = [":grpc_sfdb_service_cc"],
)

go_proto_library(
    name = "grpc_sfdb_service_go_proto",
    compiler = "@io_bazel_rules_go//proto:go_grpc",
    importpath = "github.com/googlegsa/sfdb/grpc_sfdb_service_go_proto",
    proto = ":grpc_sfdb_service_proto",
    visibility = ["//visibility:public"],
    deps = ["//sfdb:api_go_proto"],
)

cc_library(
    name = "grpc_modules",
    srcs = ["grpc_modules.h", "grpc_modules.cc"],
    hdrs = ["grpc_modules.h"],
    deps = [
        "//sfdb/base:db",
        "//sfdb/base:vars",
        "//sfdb/raft",
        "//util/time:clock",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
  name = "grpc_sfdb_service_impl",
  srcs = ["grpc_sfdb_service_impl.h", "grpc_sfdb_service_impl.cc"],
  hdrs = ["grpc_sfdb_service_impl.h"],
  deps = [
        ":grpc_modules",
        ":grpc_sfdb_service_cc_grpc",
        "//sfdb:api",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
        "@com_github_grpc_grpc//:grpc++",
  ],
)

cc_library(
  name = "grpc_sfdb_server",
  srcs = ["grpc_sfdb_server.h", "grpc_sfdb_server.cc"],
  hdrs = ["brpc_sfdb_server.h"],
  deps = [
        ":grpc_sfdb_service_impl",
        ":grpc_modules",
        ":server",
        "@com_github_google_glog//:glog",
        "@com_google_absl//absl/memory",
  ],
)