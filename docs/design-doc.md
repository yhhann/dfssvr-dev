#DFS 2.0设计#

> DFS 2.0 是新一代分布式文件存储服务，由一组 java 的客户端
> 程序和一组 golang 的服务器程序组成一个分布式文件服务集群。

##功能列表 ##

###1. 文件传送 ###
1. 文件内容直接写入到底层的存储，而不经过存储转发。
（原型中完成）
2. 服务降级，故障时，文件写入临时的本地磁盘中。  
（进行中）
3. 文件合并，多个文件可以并发写入，然后合并成一个新的文件。
（该功能延期）

###2. 服务发现 ###
1. DFSServerList，服务的注册，以及服务状态变更的通知机制。
（原型中完成）
2. 连接管理，支持以种子的方式连接服务。  TODO Priority 1.0
3. 一致性hash连接。   TODO Priority 1.0
4. 负载均衡和故障恢复。 TODO Priority 1.0

###3. 存储管理 ###
1. 存储服务列表，支持GlusterFS和GridFS等。 （原型中完成）
2. 分片管理，分片数据变更通知。  TODO Priority 1.0

###4. 元数据管理 ###
1. 支持亿级的metadata的存储方案。 TODO  Priority 0.5
2. 相同md5 文件的去重。 TODO Priority 1.0

###5. 监控管理 ###
1. DFS Server 列表和状态查看，降级管理工具。 TODO Priority 0.8
2. Shard Server 列表和状态查看，Shard管理工具。TODO Priority 0.6
3. 分片迁移工具。 TODO Priority 0.6

###6. 小文件处理 ###
1. 参考SeaweedFS的实现，或者直接使用SeaweedFS作为一个分片，
专门用来存储小文件。 TODO Priority 0.2
