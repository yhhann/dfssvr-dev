# Developer Environment Setup

## Prepare the SrcCamp client

First, install "sc" command and bazel on your dev machine.

```bash
$ sc init <client>
$ cd <client>
$ sc track bld_tools/workspace
$ sc workspace
```

## Download from SrcCamp

```bash
$ sc track dfs
$ sc deps dfs
```

## Install GlusterFS library

On Ubuntu 16.04, run this command:

```bash
$ sudo apt install glusterfs-common
```

## Build

For the first build (or when the bazel rules are updated), you need to start
"sc serve" in a different console:

```bash
$ sc serve -proxy https://50.125.238.10:3721
```

Next, build the DFS service:

```bash
$ bazel build dfs/cmd/dfs/dfssvr
```
