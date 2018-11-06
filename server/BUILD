package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    deps = [
        "//dfs/discovery:go_default_library",
        "//dfs/fileop:go_default_library",
        "//dfs/instrument:go_default_library",
        "//dfs/meta:go_default_library",
        "//dfs/metadata:go_default_library",
        "//dfs/notice:go_default_library",
        "//dfs/proto/discovery:go_default_library",
        "//dfs/proto/transfer:go_default_library",
        "//dfs/recovery:go_default_library",
        "//dfs/util:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/golang.org/x/net/context:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc/codes:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc/peer:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc/transport:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2/bson:go_default_library",
    ],
)
