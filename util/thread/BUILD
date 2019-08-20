package(default_visibility = ["//visibility:public"])

cc_library(
    name = "thread",
    srcs = [
        "thread.cc",
    ],
    hdrs = [
        "concurrent_queue.h",
        "thread.h",
    ],
    deps = [
        "//util/macros",
        "@com_github_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)
