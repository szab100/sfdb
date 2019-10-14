package(default_visibility = ["//visibility:public"])

config_setting(
    name = "varz_disable",
    values = {"define": "varz_disable=true"},
)

cc_library(
    name = "varz",
    srcs = [
        "service.h",
    ] + select({"//conditions:default" : ["service.cc", "varz.cc", "httplib.h"], ":varz_disable" : [] }),
    hdrs = [
        "varz.h"
    ],
    deps = [
        "@com_github_glog_glog//:glog",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:variant",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/synchronization",
        "//util/macros",
    ],
    defines = select({":varz_disable" : ["DISABLE_VARZ"], "//conditions:default" : [] }),
)