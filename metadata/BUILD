package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//dfs/instrument:go_default_library",
        "//dfs/proto/transfer:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2/bson:go_default_library",
    ],
)

go_test(
    name = "test",
    srcs = glob(["cachelog_test.go"]),
    embed = [":go_default_library"],
    deps = [
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2/bson:go_default_library",
    ],
)
