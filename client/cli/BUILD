package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "client",
    embed = [":go_default_library"],
)

go_library(
    name = "go_default_library",
    srcs = ["client.go"],
    importpath = "github.com/googlegsa/sfdb/client/cli",
    deps = [
        "//driver/go:driver",
        "@com_github_golang_glog//:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)
