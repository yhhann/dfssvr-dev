package sql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

type DatabaseMgr struct {
	addrs   []string
	servers map[string]*sql.DB
	conf    *DatabaseConf

	current int
	lock    sync.Mutex
}

type DatabaseConf struct {
	User   string
	Pass   string
	Addrs  string
	DBname string
}

func (mgr *DatabaseMgr) Init(conf *DatabaseConf) {
	mgr.conf = conf
	serverAddrs := strings.Split(conf.Addrs, ",")
	mgr.servers = make(map[string]*sql.DB)

	for _, a := range serverAddrs {
		dsn := conf.User + ":" + conf.Pass + "@tcp(" + a + ")/" + conf.DBname
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			glog.Errorf("Failed to connect to node %s, %v", dsn, err)
			continue
		}
		if err = db.Ping(); err != nil {
			glog.Errorf("Failed to ping node %s, %v", dsn, err)
			continue
		}

		mgr.servers[a] = db
		mgr.addrs = append(mgr.addrs, a)
	}
}

func (mgr *DatabaseMgr) Session(ctx context.Context) (*sql.DB, error) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	if len(mgr.servers) <= 0 {
		return nil, errors.New("no available session")
	}

	i := mgr.current % len(mgr.servers)
	mgr.current++

	return mgr.servers[mgr.addrs[i]], nil
}

func (mgr *DatabaseMgr) Close() {
	for a, db := range mgr.servers {
		err := db.Close()
		if err != nil {
			glog.Errorf("Close session %s error %v", a, err)
		}
	}
}

// NewDatabaseMgr returns a new database manager.
func NewDatabaseMgr() *DatabaseMgr {
	return &DatabaseMgr{}
}
