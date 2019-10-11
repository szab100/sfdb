package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "admin",
    visibility = ["//visibility:public"],
    srcs = [
        "internal/api.go",
        "internal/router.go",
    ],
    data = [
        "static/app.js",
        "static/connection.js",
        "static/home.js",
        "static/table_list.js",
        "static/table.js",
        "static/create_table.js",
        "static/view_table.js",
        "templates/home.html",
        "templates/table_list.html",
        "templates/table.html",
        "templates/create_table.html",
        "templates/view_table.html",
        "templates/index.html",
    ],
    deps = [
        "//sfdb:api_go_proto",
        "//driver/go:driver",
        "@com_github_gorilla_mux//:go_default_library",
    ],
    importpath = "github.com/googlegsa/sfdb/driver/go/admin"
)

go_binary(
    name = "admin-ui",
    srcs = [
        "cmd/main.go",
    ],
    deps = [
        ":admin",
        "//driver/go:driver",
        "@com_github_gorilla_mux//:go_default_library",
    ],
)

