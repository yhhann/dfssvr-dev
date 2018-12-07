package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"google.golang.org/grpc"

	"jingoal.com/dfs/conf"
	"jingoal.com/dfs/instrument"
	"jingoal.com/dfs/notice"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/server"
	"jingoal.com/dfs/util"
)

var (
	serverId = flag.String("server-name", "test-dfs-svr", "unique name")

	lsnAddr          = flag.String("listen-addr", ":10000", "listen address")
	zkAddr           = flag.String("zk-addr", "127.0.0.1:2181", "zookeeper address")
	zkTimeout        = flag.Uint("zk-timeout", 15000, "zookeeper timeout")
	shardDbName      = flag.String("shard-name", "shard", "shard database name")
	shardDbUri       = flag.String("shard-dburi", "mongodb://127.0.0.1:27017", "shard database uri")
	eventDbName      = flag.String("event-dbname", "dfsevent", "event database name")
	eventDbUri       = flag.String("event-dburi", "", "event database uri")
	slogDbName       = flag.String("slog-dbname", "dfsslog", "slog database name")
	slogDbUri        = flag.String("slog-dburi", "", "slog database uri")
	logFlushInterval = flag.Uint("log-flush-interval", 10, "interval of glog print in second.")
	compress         = flag.Bool("compress", false, "compressing transfer file")
	concurrency      = flag.Uint("concurrency", 0, "Concurrency")

	VERSION   = "2.0"
	buildTime = ""
)

func checkFlags() {
	if *serverId == "" {
		glog.Exit("Error: flag --server-name is required.")
	}
	if *lsnAddr == "" {
		glog.Exit("Flag --server-addr is required.")
	}
	if *zkAddr == "" {
		glog.Exit("Flag --zk-addr is required.")
	}
	if *shardDbName == "" {
		glog.Exit("Flag --shard-name is required.")
	}
	if *shardDbUri == "" {
		glog.Exit("Flag --shard-dburi is required.")
	}
	if *slogDbName == "" {
		slogDbName = shardDbName
	}
	if *slogDbUri == "" {
		slogDbUri = shardDbUri
	}
	if *eventDbName == "" {
		eventDbName = shardDbName
	}
	if *eventDbUri == "" {
		eventDbUri = shardDbUri
	}
	if *concurrency < 0 {
		*concurrency = 0
	}
}

func init() {
	glog.MaxSize = 1024 * 1024 * 32
}

// This is a DFSServer instance.
func main() {
	flag.Parse()

	zk := notice.NewDfsZK(strings.Split(*zkAddr, ","), time.Duration(*zkTimeout)*time.Millisecond)
	conf.NewConf(conf.DfssvrConfPath, conf.DfssvrPrefix, *serverId, zk)

	logFlags()

	checkFlags()

	go flushLogDaemon()
	defer glog.Flush()

	term := make(chan os.Signal, 1)
	retire := make(chan bool, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)

	instrument.StartMetrics()

	lis, err := net.Listen("tcp", *lsnAddr)
	if err != nil {
		glog.Exitf("failed to listen %v", err)
	}
	glog.Infof("DFSServer listened on %s", lis.Addr().String())

	dbAddr := &server.DBAddr{
		ShardDbName: *shardDbName,
		ShardDbUri:  *shardDbUri,
		EventDbName: *eventDbName,
		EventDbUri:  *eventDbUri,
		SlogDbName:  *slogDbName,
		SlogDbUri:   *slogDbUri,
	}

	var dfsServer *server.DFSServer
	for {
		transfer.ServerId = *serverId
		dfsServer, err = server.NewDFSServer(lis.Addr(), *serverId, dbAddr, zk)
		if err != nil {
			glog.Warningf("Failed to create DFS Server: %v, try again.", err)
			time.Sleep(time.Duration(*server.HealthCheckInterval) * time.Second)
			continue
		}

		break
	}

	sopts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(uint32(*concurrency)),
	}

	if *compress {
		sopts = append(sopts, grpc.RPCCompressor(grpc.NewGZIPCompressor()))
		sopts = append(sopts, grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	}

	sopts = append(sopts, grpc.UnaryInterceptor(util.UnaryRecoverServerInterceptor))
	sopts = append(sopts, grpc.StreamInterceptor(util.StreamRecoverServerInterceptor))

	grpcServer := grpc.NewServer(sopts...)
	transfer.RegisterFileTransferServer(grpcServer, dfsServer)
	discovery.RegisterDiscoveryServiceServer(grpcServer, dfsServer)

	glog.Flush()

	go func() {
		select {
		case <-term:
			glog.Infoln("Start to shutdown DFS server ...")
			dfsServer.Unregister()

			go func() {
				startToClose := false
				ticker := time.Tick(time.Second)
				for {
					select {
					case <-ticker:
						p := instrument.GetInProcess()
						if p == 0 && !startToClose {
							startToClose = true
							go func() {
								ticker := time.Tick(3 * time.Second)
								select {
								case <-ticker:
									retire <- true
								}
							}()
						}

						glog.Infof("Number of task in process: %d.", p)
						glog.Flush()
					}
				}
			}()

			grpcServer.GracefulStop()

			retire <- true
		}
	}()

	grpcServer.Serve(lis)

	<-retire
	glog.Infoln("DFS server stopped gracefully.")

	glog.Flush()
}

func flushLogDaemon() {
	for range time.Tick(time.Duration(*logFlushInterval) * time.Second) {
		glog.Flush()
	}
}

func logFlags() {
	glog.Infoln("flags:")
	flag.VisitAll(func(f *flag.Flag) {
		glog.Infof("\t%s->%s\n", f.Name, f.Value)
	})
	glog.Flush()
}
