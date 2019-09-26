package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

go_library(
    name = "driver",
    srcs = [
        "column_type.go",
        "const.go",
        "connection.go",
        "driver.go",
        "result.go",
        "rows.go",
        "rpc_executor.go",
        "statement.go",
    ],
    deps = [
        "//sfdb:api_go_proto",
        "@com_github_golang_glog//:go_default_library",
        "@com_github_golang_protobuf//ptypes/any:go_default_library",
        "@com_github_golang_protobuf//protoc-gen-go/descriptor:go_default_library",
        "@com_github_jhump_protoreflect//dynamic:go_default_library",
        "@com_github_jhump_protoreflect//desc:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
    importpath = "github.com/googlegsa/sfdb/driver/go"
)

# TODO: mock gRPC connection, so this won't require a running server instance
# go_test(
#     name = "driver_test",
#     srcs = ["driver_test.go"],
#     embed = [":driver"],
#)

go_test(
    name = "connection_test",
    srcs = ["connection_test.go"],
    embed = [":driver"],
)

