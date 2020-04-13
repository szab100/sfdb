py_binary(
  name = "main",
  srcs = ["main.py",],
  python_version = "PY3",
  deps = [
    "//driver/python:py_db_api",
    ":lib",
  ]
)

py_library(
  name = "lib",
  srcs = [
    "__init__.py",
    "optionparser.py",
    "sfdb_cli.py",
  ],
  visibility = ["//visibility:public"],
  imports = ["."],
)

py_test(
  name = "test/sfdbcli_test",
  srcs = ["test/sfdbcli_test.py",],
  python_version = "PY3",
  visibility = ["//visibility:public"],
  deps = [
    "//driver/python:py_db_api",
    "//driver/python:test/py_test_lib",
  ],
  data = ["//sfdb:sfdb"],
)