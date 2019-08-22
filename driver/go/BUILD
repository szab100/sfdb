package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "driver",
    srcs = [
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
        "@org_golang_google_grpc//:go_default_library",
    ],
    importpath = "github.com/googlegsa/sfdb/driver/go"
)

go_test(
    name = "driver_test",
    srcs = [
        "driver_test.go",
    ],
    embed = [":driver"],
)

go_test(
    name = "connection_test",
    srcs = [
        "connection_test.go",
    ],
    embed = [":driver"],
)

