package(default_visibility = ["//visibility:public"])

cc_library(
    name = "async_wrappers",
    srcs = [
        "async_response_reader.h",
        "async_stub_wrapper.h",
        "completion_callback.h",
    ],
    hdrs = [
        "async_response_reader.h",
        "async_stub_wrapper.h",
        "completion_callback.h",
    ],
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "async_stub_testing",
    srcs = [
        "async_stub_testing.h",
    ],
    hdrs = [
        "async_stub_testing.h",
    ],
    deps = [
        ":async_wrappers",
        "@com_github_google_glog//:glog",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)
