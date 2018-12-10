#!/bin/sh

docker run -ti --name dfs-builder --mount type=bind,source=/home/yhhan/,target=/ws dfs-builder:1.0 bash
