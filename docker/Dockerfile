# 产线的 GlusterFS 使用的是 3.5.1 版本，本 Dockerfile 基于 ubnutu 15.04 进行构建，
# 通过安装 glusterfs-client-3.5.2 来最大程度的仿真线上环境。
# Author: yhhanec@126.com
#
FROM ubuntu-core-15.04-core-amd64

ENV DEBIAN_FRONTEND noninteractive

COPY --chown=0:0 sources.list /etc/apt/
COPY glusterfs-*.deb /tmp/

# 以下的操作，先从 apt 源安装 glusterfs 的高版本，
# 安装之后卸载，再用 deb 文件安装 glusterfs-3.5.2 版本。
# 由于编译 DFS Server 需要使用 CGO, 因此需要安装 gcc 等组件.
RUN buildDeps='gcc libc6-dev make pkg-config glusterfs-common glusterfs-client' \
              && apt-get update \
              && apt-get install -y --no-install-recommends apt-utils \
              && apt-get install -y $buildDeps \
              && apt-get purge -y glusterfs-common glusterfs-client \ 
              && dpkg -i /tmp/glusterfs-*.deb \
              && apt-get clean \
              && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /tmp/glusterfs-*.deb

ENV DEBIAN_FRONTEND teletype
