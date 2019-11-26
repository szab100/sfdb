package(default_visibility = ["//visibility:public"])

cc_library(
    name = "proto",
    srcs = [
        "parse_text_proto.cc",
    ],
    hdrs = [
        "parse_text_proto.h",
        "matchers.h",
    ],
    deps = [
        "@com_google_absl//absl/strings",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
    ],
)
