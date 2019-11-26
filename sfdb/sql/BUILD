package(default_visibility = ["//visibility:public"])

# ------------------------------------------------------------------------------
# Libraries
# ------------------------------------------------------------------------------

cc_library(
    name = "parser",
    srcs = ["parser.cc"],
    hdrs = ["parser.h"],
    deps = [
        ":tokenizer",
        "//util/types",
        "//sfdb/base:ast",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "tokenizer",
    srcs = ["tokenizer.cc"],
    hdrs = ["tokenizer.h"],
    deps = [
        "//util/types",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_absl//absl/strings",
    ],
)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

cc_test(
    name = "parser_test",
    size = "small",
    srcs = ["parser_test.cc"],
    deps = [
        ":parser",
        "//sfdb/base:ast",
        "//sfdb/base:value",
        "//util/task:status",
        "//util/task:statusor",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "tokenizer_test",
    size = "small",
    srcs = ["tokenizer_test.cc"],
    deps = [
        ":tokenizer",
        "//util/task:status",
        "//util/task:statusor",
        "@com_github_google_glog//:glog",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
