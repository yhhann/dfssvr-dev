package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_docker//docker:docker.bzl", "docker_build")
load("@io_bazel_rules_go//go:def.bzl", "go_binary")

go_binary(
    name = "dfssvr",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    x_defs = {
        "main.buildTime": "2016/06/30_00:00",
    },
    deps = [
        "//dfs/conf:go_default_library",
        "//dfs/instrument:go_default_library",
        "//dfs/notice:go_default_library",
        "//dfs/proto/discovery:go_default_library",
        "//dfs/proto/transfer:go_default_library",
        "//dfs/server:go_default_library",
        "//dfs/util:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc:go_default_library",
    ],
)

docker_build(
    name = "latest",
    base = "//bld_tools/docker/glusterfs:latest",
    cmd = "/dfs/bin/start.sh",
    directory = "/dfs/bin",
    files = [
        "start.sh",
        ":dfssvr",
    ],
    repository = "harbor.eff.com",
    tag = "dfs/server",
    workdir = "/dfs",
)
