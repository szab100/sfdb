package(default_visibility = ["//visibility:public"])

proto_library(
    name = "codes",
    srcs = ["codes.proto"],
)

cc_proto_library(
    name = "codes_cpp",
    deps = [":codes"],
)

cc_library(
    name = "status",
    srcs = [
        "status.cc",
        "canonical_errors.cc",
    ],
    hdrs = [
        "status.h",
        "canonical_errors.h",
        "error_space.h",
    ],
    deps = [
        ":codes_cpp",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings",
        "@com_github_glog_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "statusor",
    hdrs = [
        "statusor.h",
    ],
    deps = [
        ":status",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "status_matchers",
    hdrs = [
        "status_matchers.h",
    ],
    deps = [
        ":status",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
