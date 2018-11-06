package server

import (
	"strings"
	"time"

	"github.com/golang/glog"

	"jingoal.com/dfs/proto/discovery"
)

// GetDfsServers gets a list of DfsServer from server.
func (s *DFSServer) GetDfsServers(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")

	observer := make(chan struct{}, 100)
	s.register.AddObserver(observer, clientId)

	glog.Infof("Client %s connected.", clientId)

	ticker := time.NewTicker(time.Duration(*heartbeatInterval) * time.Second)
	defer ticker.Stop()

outLoop:
	for {
		select {
		case _, ok := <-observer:
			if !ok {
				glog.Infof("Succeeded to close stream to %s", clientId)
				return nil
			}
			if err := s.sendDfsServerMap(req, stream); err != nil {
				glog.Warningf("Failed to send server list to %s, %v.", clientId, err)
				break outLoop
			}
		case <-ticker.C:
			if err := s.sendHeartbeat(req, stream); err != nil {
				glog.Warningf("Failed to send heart beat to %s, %v.", clientId, err)
				break outLoop
			}
		}
	}

	s.register.RemoveObserver(observer)
	glog.Infof("Client connection closed, client: %s", clientId)

	return nil
}

func (s *DFSServer) sendHeartbeat(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Hb{
			Hb: &discovery.Heartbeat{
				Timestamp: time.Now().Unix(),
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	return nil
}

func (s *DFSServer) sendDfsServerMap(req *discovery.GetDfsServersReq, stream discovery.DiscoveryService_GetDfsServersServer) error {
	sm := s.register.GetDfsServerMap()
	ss := make([]*discovery.DfsServer, 0, len(sm))
	for _, pd := range sm {
		// If we detect a server offline, we set its value to nil,
		// so we must filter nil values out.
		if pd != nil {
			ss = append(ss, pd)
		}
	}

	rep := &discovery.GetDfsServersRep{
		GetDfsServerUnion: &discovery.GetDfsServersRep_Sl{
			Sl: &discovery.DfsServerList{
				Server: ss,
			},
		},
	}

	if err := stream.Send(rep); err != nil {
		return err
	}

	clientId := strings.Join([]string{req.GetClient().Id, getPeerAddressString(stream.Context())}, "/")
	glog.Infof("Succeeded to send dfs server list to client: %s, Servers:", clientId)
	for i, s := range ss {
		glog.Infof("\t\t%d. DfsServer: %s\n", i+1, strings.Join([]string{s.Id, s.Uri, s.Status.String()}, "/"))
	}

	return nil
}
