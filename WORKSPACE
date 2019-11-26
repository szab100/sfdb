workspace(name = "com_github_google_sfdb")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Protobuf
# NOTE: a particular version is needed because of BRAFT/BRPC
http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.6.1.3",
    sha256 = "9510dd2afc29e7245e9e884336f848c8a6600a14ae726adb6befdb4f786f0be2",
    type = "zip",
    url = "https://github.com/protocolbuffers/protobuf/archive/v3.6.1.3.zip",
)

#load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

#protobuf_deps()

# gRPC
git_repository(
    name = "com_github_grpc_grpc",
    remote = "https://github.com/grpc/grpc.git",
    commit = "08fd59f039c7cf62614ab7741b3f34527af103c7",
    shallow_since = "1562093080 -0700"
)
git_repository( # transitive dependency of gRPC - fixing version to
    name = "boringssl",
    remote = "https://github.com/google/boringssl.git",
    commit = "a21f78d24bf645ccd6774b2c7e52e3c0514f7f29",
    shallow_since = "1565026374 +0000"
)
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()

# Various Git Repositories
git_repository(
    name = "com_github_google_glog",
    remote = "https://github.com/google/glog.git",
    commit = "3106945d8d3322e5cbd5658d482c9ffed2d892c0",
    #shallow_since = "1516118775 +0100"
)

git_repository(
    name = "com_google_absl",
    remote = "https://github.com/abseil/abseil-cpp",
    commit = "36d37ab992038f52276ca66b9da80c1cf0f57dc2",
    shallow_since = "1564092152 -0400"
)

git_repository(
    name = "com_google_googletest",
    remote = "https://github.com/google/googletest",
    commit = "2fe3bd994b3189899d93f1d5a881e725e046fdc2",
    shallow_since = "1535728917 -0400"
)

git_repository(
    name = "com_github_google_re2",
    commit = "848dfb7e1d7ba641d598cb66f81590f3999a555a",
    remote = "https://github.com/google/re2.git",
    shallow_since = "1560490505 +0000",
)

# GoLang main Bazel tools
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "8df59f11fb697743cbb3f26cfb8750395f30471e9eabde0d174c3aebc7a1cd39",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/0.19.1/rules_go-0.19.1.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/0.19.1/rules_go-0.19.1.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

# Go repositories
http_archive(
    name = "bazel_gazelle",
    sha256 = "be9296bfd64882e3c08e3283c58fcb461fa6dd3c171764fcc4cf322f60615a9b",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/0.18.1/bazel-gazelle-0.18.1.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/0.18.1/bazel-gazelle-0.18.1.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/grpc",
    sum = "h1:J0UbZOIrCAl+fpTOf8YLs4dJo8L/owV4LYVtAXQoPkw=",
    version = "v1.22.0",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    sum = "h1:oWX7TPOiFAMXLq8o0ikBYfCJVlRHBcsciT5bXOrH628=",
    version = "v0.0.0-20190311183353-d8887717615a",
)

go_repository(
    name = "org_golang_x_text",
    importpath = "golang.org/x/text",
    sum = "h1:g61tztE5qeGQ89tm6NTjjM9VPIm088od1l6aSorWRWg=",
    version = "v0.3.0",
)

go_repository(
    name = "com_github_golang_glog",
    importpath = "github.com/golang/glog",
    sum = "h1:VKtxabqXZkF25pY9ekfRL6a582T4P37/31XEstQ5p58=",
    version = "v0.0.0-20160126235308-23def4e6c14b",
)

go_repository(
    name = "com_github_jhump_protoreflect",
    importpath = "github.com/jhump/protoreflect",
    sum = "h1:NgpVT+dX71c8hZnxHof2M7QDK7QtohIJ7DYycjnkyfc=",
    version = "v1.5.0",
)

go_repository(
    name = "com_github_golang_protobuf",
    importpath = "github.com/golang/protobuf",
    sum = "h1:6nsPYzhq5kReh6QImI3k5qWzO4PEbvbIW2cwSfR/6xs=",
    version = "v1.3.2",
)

go_repository(
    name = "com_github_gorilla_mux",
    importpath = "github.com/gorilla/mux",
    sum = "h1:gnP5JzjVOuiZD07fKKToCAOjS0yOpj/qPETTXCCS6hw=",
    version = "v1.7.3",
)

git_repository(
    name = "com_github_brpc_braft",
    remote = "https://github.com/brpc/braft",
    commit = "d0277bf2aea66908d1fd7376d191bd098371966e"
)

http_archive(
    name = "com_github_brpc_brpc",
    strip_prefix = "incubator-brpc-2b748f82c3447196c8ce372733e5af8f8d76cef5",
    url = "https://github.com/apache/incubator-brpc/archive/2b748f82c3447196c8ce372733e5af8f8d76cef5.tar.gz",
)

http_archive(
    name = "com_github_google_leveldb",
    build_file = "@com_github_brpc_braft//:leveldb.BUILD",
    strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
    url = "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"
)

