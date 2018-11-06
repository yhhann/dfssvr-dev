FROM docker-registry.eff.com/centos/centos:6.6
MAINTAINER yhhan

# install gluster client api.
RUN /usr/bin/yum -y install \
    uuid \
    glusterfs-libs-3.5.1-1.el6.x86_64 \
    glusterfs-3.5.1-1.el6.x86_64 \
    glusterfs-api-3.5.1-1.el6.x86_64

# copy dfssvr
COPY bin/dfssvr  /dfs/bin/dfssvr
COPY bin/start.sh /dfs/bin/start.sh
CMD ["/dfs/bin/start.sh"]
