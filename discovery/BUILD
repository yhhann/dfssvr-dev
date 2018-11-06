package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//dfs/notice:go_default_library",
        "//dfs/proto/discovery:go_default_library",
        "//dfs/proto/transfer:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
    ],
)
