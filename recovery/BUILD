package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//dfs/metadata:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2/bson:go_default_library",
    ],
)
