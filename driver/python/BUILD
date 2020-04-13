# bazel test //driver/python:all_tests --test_output=streamed --runs_per_test=2

py_library(
  name = "py_db_api",
  srcs = [
    "__init__.py",
    "connection.py",
    "cursor.py",
    "exceptions.py",
    "dbtypes.py",
  ],
  srcs_version = "PY3",
  visibility = ["//visibility:public"],
  imports = ["."],
  deps = [
    "//server:grpc_sfdb_service_py_pb2",
    "//server:grpc_sfdb_service_py_pb2_grpc",
    "//sfdb:api_py_pb2",
    "//sfdb:api_pb2_grpc",
  ],
)

py_library(
  name = "test/py_test_lib",
  srcs = [
    "test/pylib.py",
    "test/test_data.py",
  ],
  srcs_version = "PY3",
  visibility = ["//visibility:public"],
)

py_test(
  name = "test/connection_test",
  srcs = [
    "test/connection_test.py",
  ],
  srcs_version = "PY3",
  visibility = ["//visibility:public"],
  deps = [
    ":py_db_api",
    ":test/py_test_lib",
  ],
  data = ["//sfdb:sfdb"],
)

py_test(
  name = "test/cursor_test",
  srcs = [
    "test/cursor_test.py",
  ],
  srcs_version = "PY3",
  visibility = ["//visibility:public"],
  deps = [
    ":py_db_api",
    ":test/py_test_lib",
  ],
  data = ["//sfdb:sfdb"],
)

test_suite(
  name = "all_tests",
)

# python toolchain definition,
# see https://github.com/bazelbuild/bazel/issues/7899
load("@bazel_tools//tools/python:toolchain.bzl", "py_runtime_pair")
py_runtime(
  name = "py3_runtime",
  interpreter_path = "/usr/bin/python3.7",
  python_version = "PY3",
)

py_runtime_pair(
  name = "py_runtime_pair",
  py3_runtime = ":py3_runtime",
)

toolchain(
  name = "py_toolchain",
  #target_compatible_with = [...], # optional platform constraints
  toolchain = ":py_runtime_pair",
  toolchain_type = "@bazel_tools//tools/python:toolchain_type",
)