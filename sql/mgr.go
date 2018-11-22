package sql

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

type DatabaseMgr struct {
	DSNs    []string
	addrs   []string
	servers map[string]*FOperator

	current int
	lock    sync.RWMutex
}

func (mgr *DatabaseMgr) init(dsns []string) {
	mgr.servers = make(map[string]*FOperator)

	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	for _, dsn := range dsns {
		cfg, err := mysql.ParseDSN(dsn)
		a := cfg.Addr
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			glog.Errorf("Failed to connect to node %s, %v", dsn, err)
			continue
		}
		if err = db.Ping(); err != nil {
			glog.Errorf("Failed to ping node %s, %v", dsn, err)
			continue
		}

		mgr.servers[a] = NewFOperator(db)
		mgr.addrs = append(mgr.addrs, a)
	}
}

func (mgr *DatabaseMgr) Session(ctx context.Context) *FOperator {
	for {
		op, err := mgr.session(ctx)
		if err == nil {
			return op
		}

		glog.Warningf("Waiting for db to be initialled, %v", err)
		time.Sleep(time.Second)
	}

	return nil
}

func (mgr *DatabaseMgr) session(ctx context.Context) (*FOperator, error) {
	mgr.lock.RLock()
	defer mgr.lock.RUnlock()

	if len(mgr.servers) <= 0 {
		return nil, errors.New("no available session")
	}

	i := mgr.current % len(mgr.servers)
	mgr.current++

	return mgr.servers[mgr.addrs[i]], nil
}

func (mgr *DatabaseMgr) Close() {
	for a, op := range mgr.servers {
		if err := op.Close(); err != nil {
			glog.Errorf("Failed to close session %s, error %v", a, err)
		}
	}
}

// NewDatabaseMgr returns a new database manager.
func NewDatabaseMgr(dsns []string) *DatabaseMgr {
	mgr := &DatabaseMgr{
		DSNs: dsns,
	}
	mgr.init(dsns)

	return mgr
}
