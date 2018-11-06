package discovery

import (
	pb "jingoal.com/dfs/proto/discovery"
)

// Selector is an interface which represents the DfsServer Selector
type Selector interface {
	GetPerfectServer() (*pb.DfsServer, error)
}
