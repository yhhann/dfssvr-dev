package notice

import (
	"path/filepath"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	// Watchers on ShardServerPath will be noticed when storage server node changed.
	// compatible with old version.
	ShardServerPath = "/shard/server"

	// Watchers on ShardChunkPath will be noticed when segment changed,
	// compatible with old version.
	ShardChunkPath = "/shard/chunk"

	// Watchers on ShardDfsPath will be noticed when other DFSServer changed.
	ShardDfsPath = "/shard/dfs"

	// This two for file migration.
	NodePath   = "/shard/nodes"
	NoticePath = "/shard/notice"
)

// DfsZK implements Notice interface.
type DfsZK struct {
	Addrs []string

	*zk.Conn
}

func (k *DfsZK) connectZk(addrs []string, timeout time.Duration) error {
	k.Addrs = addrs
	thisConn, ch, err := zk.Connect(zk.FormatServers(addrs), timeout)
	if err != nil {
		return err
	}

	connectOk := make(chan struct{})
	go k.checkConnectEvent(ch, connectOk)
	<-connectOk
	k.Conn = thisConn

	k.ensurePathExist(filepath.Dir(ShardDfsPath))

	return nil
}

// CloseZK closes the zookeeper.
func (k *DfsZK) CloseZk() {
	if k != nil {
		k.Close()
		k = nil
	}
}

func (k *DfsZK) checkConnectEvent(ch <-chan zk.Event, okChan chan<- struct{}) {
	for ev := range ch {
		switch ev.Type {
		case zk.EventSession:
			switch ev.State {
			case zk.StateConnecting:
			case zk.StateConnected:
				glog.Infof("Succeeded to connect to zk[%v].", k.Addrs)
			case zk.StateHasSession:
				glog.Infof("Succeeded to get session from zk[%v].", k.Addrs)
				okChan <- struct{}{}
			}
		default:
		}
	}
}

// GetChildren gets the name of children under the given path.
func (k *DfsZK) GetChildren(path string) ([]string, error) {
	k.ensurePathExist(path)

	result, _, err := k.Children(path)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// CheckChildren sets a watcher on given path,
// the returned chan will be noticed when children changed.
func (k *DfsZK) CheckChildren(path string) (<-chan []string, <-chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)

	k.ensurePathExist(path)
	go func() {
		for {
			snapshot, _, events, err := k.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot

			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
			glog.V(4).Infof("zk event %+v.", evt)
		}
	}()

	return snapshots, errors
}

// CheckDataChange sets a watcher on given path,
// the returned chan will be noticed when data changed.
func (k *DfsZK) CheckDataChange(path string) (<-chan []byte, <-chan error) {
	datas := make(chan []byte)
	errors := make(chan error)

	k.ensurePathExist(path)
	go func() {

		for {
			dataBytes, _, events, err := k.GetW(path)
			if err != nil {
				errors <- err
				return
			}
			datas <- dataBytes
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}

		}
	}()

	return datas, errors
}

// GetData returns the data of given path.
func (k *DfsZK) GetData(path string) ([]byte, error) {
	data, _, err := k.Get(path)
	return data, err
}

func (k *DfsZK) createEphemeralSequenceNode(prefix string, data []byte) (string, error) {
	flags := int32(zk.FlagEphemeral | zk.FlagSequence)
	acl := zk.WorldACL(zk.PermAll)
	path, err := k.Create(prefix, data, flags, acl)
	if err != nil {
		return "", err
	} else {
		return path, nil
	}
}

// Unregister unregisters a server.
func (k *DfsZK) Unregister(node string) error {
	fullPath := filepath.Join(ShardDfsPath, node)
	return k.Delete(fullPath, -1)
}

// Register registers a server.
// if check is true, the returned chan will be noticed when sibling changed.
func (k *DfsZK) Register(prefix string, data []byte, startCheckRoutine bool) (string, <-chan []byte, <-chan error, <-chan struct{}, <-chan struct{}) {
	siblings, errs := k.CheckChildren(filepath.Dir(prefix))

	results := make(chan []byte)
	errors := make(chan error)
	clearFlag := make(chan struct{})
	sendFlag := make(chan struct{})

	if startCheckRoutine {
		go func() {
			for {
				select {
				case sn := <-siblings:
					sort.Sort(sort.StringSlice(sn))

					clearFlag <- struct{}{}

					for _, s := range sn {
						path := filepath.Join(filepath.Dir(prefix), s)
						d, err := k.GetData(path)
						if err != nil {
							glog.Warningf("node lost %v, %v", path, err)
							errors <- err
							continue
						}

						results <- d
					}

					sendFlag <- struct{}{}

				case err := <-errs:
					errors <- err
					return
				}
			}
		}()
	}

	path, err := k.createEphemeralSequenceNode(prefix, data)
	if err != nil {
		errors <- err
	}
	return path, results, errors, clearFlag, sendFlag
}

func (k *DfsZK) ensurePathExist(path string) error {
	if exists, _, _ := k.Exists(path); exists {
		return nil
	}
	_, err := k.Create(path, []byte(filepath.Base(path)), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		glog.Infof("Ensure zk node %s, %v.", path, err)
		if err == zk.ErrNodeExists {
			return nil
		}
		if err == zk.ErrNotEmpty {
			return nil
		}
		return err
	}
	glog.Infof("Zk node %s ensured.", path)

	return nil
}

// NewDfsZK creates a new DfsZk.
func NewDfsZK(addrs []string, timeout time.Duration) *DfsZK {
	zk := new(DfsZK)
	if err := zk.connectZk(addrs, timeout); err != nil {
		return nil
	}
	return zk
}
