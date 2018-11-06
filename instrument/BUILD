package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//third-party-go/vendor/github.com/prometheus/client_golang/prometheus:go_default_library",
        "//third-party-go/vendor/github.com/prometheus/client_model/go:go_default_library",
    ],
)
