// Package server implements DFSServer.
package server

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/peer"

	disc "jingoal.com/dfs/discovery"
	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/notice"
	"jingoal.com/dfs/proto/discovery"
	"jingoal.com/dfs/proto/transfer"
	"jingoal.com/dfs/recovery"
)

var (
	logDir            = flag.String("gluster-log-dir", "/var/log/dfs", "gluster log file dir")
	heartbeatInterval = flag.Int("hb-interval", 5, "time interval in seconds of heart beat")
	enablePreJudge    = flag.Bool("enable-prejudge", false, "enable timeout pre-judge.")

	RegisterAddr    = flag.String("register-addr", "", "register address")
	DefaultDuration = flag.Int("default-duration", 5, "default transfer duration in seconds.")
	preferred       = flag.String("preferred", "", "preferred domains split by comma.")
)

var (
	AssertionError = errors.New("assertion error")
)

var (
	shardAddr *DBAddr
)

// DBAddr represents a bundle of mongodb addresses, including
// the address of shard, event and space log.
type DBAddr struct {
	ShardDbName string
	ShardDbUri  string
	EventDbName string
	EventDbUri  string
	SlogDbName  string
	SlogDbUri   string
}

// DFSServer implements DiscoveryServiceServer and FileTransferServer.
type DFSServer struct {
	mOp      metadata.MetaOp
	spaceOp  *metadata.SpaceLogOp
	eventOp  *metadata.EventOp
	cacheOp  *metadata.CacheLogOp
	reOp     *recovery.RecoveryEventOp
	register disc.Register
	notice   notice.Notice
	selector *HandlerSelector
}

// Unregister closes connection of registered client
// and unregister self from zk.
func (s *DFSServer) Unregister() {
	if s == nil {
		return
	}

	if s.register != nil {
		s.register.Unregister() // ignore error
		s.register.Close()
	}
}

// Close releases resource held by DFSServer.
func (s *DFSServer) Close() {
	if s == nil {
		return
	}

	if s.mOp != nil {
		s.mOp.Close()
	}
	if s.spaceOp != nil {
		s.spaceOp.Close()
	}
	if s.eventOp != nil {
		s.eventOp.Close()
	}
	if s.reOp != nil {
		s.reOp.Close()
	}
	if s.cacheOp != nil {
		s.cacheOp.Close()
	}
	if s.notice != nil {
		s.notice.CloseZk()
	}
}

func (s *DFSServer) registerSelf(lsnAddr string, name string) error {
	glog.Infof("Start to register self[%s,%s]", name, lsnAddr)

	rAddr, err := sanitizeLsnAddr(lsnAddr)
	if err != nil {
		return err
	}

	dfsServer := &discovery.DfsServer{
		Id:     name,
		Uri:    rAddr,
		Status: discovery.DfsServer_ONLINE,
	}
	p := strings.TrimSpace(*preferred)
	if len(p) > 0 {
		pre := strings.Split(p, ",")
		preferred := make([]string, 0, len(pre))
		for _, s := range pre {
			ss := strings.TrimSpace(s)
			if len(ss) == 0 {
				continue
			}
			preferred = append(preferred, ss)
		}

		// 0 has lowest priority, and 1 higher than 0.
		// We will use the priority and preferred in loadbalance of client.
		dfsServer.Priority = 1
		dfsServer.Preferred = preferred
	}

	if err := s.register.Register(dfsServer); err != nil {
		return err
	}

	glog.Infof("Succeeded to register self[%s,%s] on %s ok", name, rAddr, transfer.NodeName)
	return nil
}

// NewDFSServer creates a DFSServer
//
// example:
//  lsnAddr, _ := ResolveTCPAddr("tcp", ":10000")
//  dfsServer, err := NewDFSServer(lsnAddr, "mySite", "shard",
//         "mongodb://192.168.1.15:27017", zk)
func NewDFSServer(lsnAddr net.Addr, name string, dbAddr *DBAddr, zk *notice.DfsZK) (server *DFSServer, err error) {
	glog.Infof("Try to start DFS server %v on %v\n", name, lsnAddr.String())

	shardAddr = dbAddr
	server = &DFSServer{
		notice:   zk,
		register: disc.NewZKDfsServerRegister(zk),
	}

	defer func() {
		if err != nil && server != nil {
			server.Close()
			server = nil
		}
	}()

	spaceOp, err := metadata.NewSpaceLogOp(dbAddr.SlogDbName, dbAddr.SlogDbUri)
	if err != nil {
		return nil, fmt.Errorf("%v, %s %s", err, dbAddr.SlogDbName, dbAddr.SlogDbUri)
	}
	server.spaceOp = spaceOp

	eventOp, err := metadata.NewEventOp(dbAddr.EventDbName, dbAddr.EventDbUri)
	if err != nil {
		return nil, fmt.Errorf("%v, %s %s", err, dbAddr.EventDbName, dbAddr.EventDbUri)
	}
	server.eventOp = eventOp

	cacheOp, err := metadata.NewCacheLogOp(dbAddr.EventDbName, dbAddr.EventDbUri)
	if err != nil {
		return nil, fmt.Errorf("%v, %s %s", err, dbAddr.EventDbName, dbAddr.EventDbUri)
	}
	server.cacheOp = cacheOp

	// Create NewMongoMetaOp
	mop, err := metadata.NewMongoMetaOp(dbAddr.ShardDbName, dbAddr.ShardDbUri)
	if err != nil {
		return nil, fmt.Errorf("%v, %s %s", err, dbAddr.ShardDbName, dbAddr.ShardDbUri)
	}
	server.mOp = mop

	reop, err := recovery.NewRecoveryEventOp(dbAddr.EventDbName, dbAddr.EventDbUri)
	if err != nil {
		return nil, fmt.Errorf("%v, %s %s", err, dbAddr.EventDbName, dbAddr.EventDbUri)
	}
	server.reOp = reop

	server.selector, err = NewHandlerSelector(server)
	glog.Infof("Succeeded to initialize storage servers.")

	// Register self.
	regAddr := *RegisterAddr
	if regAddr == "" {
		regAddr = lsnAddr.String()
	}

	if err := server.registerSelf(regAddr, name); err != nil {
		return nil, err
	}

	p := strings.TrimSpace(*preferred) // borrow a flag
	if len(p) > 0 {
		server.selector.startCachedFileRecoveryRoutine()
	}

	server.selector.startRecoveryDispatchRoutine()
	server.selector.startShardNoticeRoutine()
	startRateCheckRoutine()

	glog.Infof("Succeeded to start DFS server '%s'.", name)

	return server, nil
}

func getIfcAddr() ([]string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	ifaddrs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			ip := ipnet.IP
			if !ip.IsLoopback() && ip.To4() != nil {
				ifaddrs = append(ifaddrs, ip.String())
			}
		}
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("get addr error")
	}

	return ifaddrs, nil
}

func sanitizeLsnAddr(lsnAddr string) (string, error) {
	ss := strings.Split(lsnAddr, ":")

	lstPort := "10000"
	if len(ss) > 1 {
		lstPort = ss[len(ss)-1]
	}

	registerIp := ss[0]

	if registerIp == "" {
		lstIps, err := getIfcAddr()
		if err != nil {
			return "", err
		}
		if len(lstIps) == 0 {
			return "", fmt.Errorf("no interface address, use loopback")
		}
		registerIp = lstIps[0]
	}

	return fmt.Sprintf("%s:%s", registerIp, lstPort), nil
}

func getPeerAddressString(ctx context.Context) (peerAddr string) {
	if per, ok := peer.FromContext(ctx); ok {
		peerAddr = per.Addr.String()
	}

	return
}
