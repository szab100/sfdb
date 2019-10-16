package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "driver",
    srcs = [
        "column_type.go",
        "connection.go",
        "const.go",
        "driver.go",
        "result.go",
        "rows.go",
        "rpc_executor.go",
        "statement.go",
    ],
    importpath = "github.com/googlegsa/sfdb/driver/go",
    deps = [
        "//sfdb:api_go_proto",
        "@com_github_golang_glog//:go_default_library",
        "@com_github_jhump_protoreflect//desc:go_default_library",
        "@com_github_jhump_protoreflect//dynamic:go_default_library",
        "@io_bazel_rules_go//proto/wkt:any_go_proto",
        "@io_bazel_rules_go//proto/wkt:descriptor_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//connectivity:go_default_library",
    ],
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

