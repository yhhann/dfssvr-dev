## Discovery 设计##
 
### 1. 设计要点 ###

#### 1. gRPC接口GetDfsServer的设计 ####
GetDfsServer接口的proto声明:

rpc GetDfsServers(GetDfsServersReq) returns (stream GetDfsServersRep)

传入的参数对调用的 Client 进行描述，以便 Server 端做日志等操作。

返回的结果是一个 grpc 的 stream 类型，这种类型是一个持续的类型，
意图是当 Server 端发现有新节点上线或者有节点下线的事件时，可以
及时向 Client 端推送最新的 DfsServer 列表。

DfsServer 发现其它节点的变化，是通过zookeeper来实现的，通过
channel 来通知，而一个 DfsServer 会对应多个 DfsClient， 这样
通过 channel 发送来的一个事件，需要针对每一个 DfsClient 生成
一个新的channel。为了实现这个功能，在原来代码的基础上，为
Register 增加了AddObserver和RemoveObserver两个方法, 分别用来
向Register注册客户端的channel。

当一个 DfsClient 建立链接之后, 首先会调用 GetDfsServers()
来获取当前可用的DfsServer列表，此时会调用AddObserver()注册
一个 channel， 并在该channel上监听，每一次监听到 channel
上有数据，就向 DfsClient 推送最新的 DfsServer列表。

2016.2.19

