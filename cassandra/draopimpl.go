package cassandra

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/relops/cqlr"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
)

const (
	cqlAttrUser        = "user"        // string
	cqlAttrPass        = "pass"        // string
	cqlAttrKeyspace    = "keyspace"    // string
	cqlAttrConsistency = "consistency" // string
	cqlAttrPort        = "port"        // integer
	cqlAttrTimeout     = "timeout"     // integer
	cqlAttrConns       = "conns"       // integer
)

const (
	// table name.
	FILES_COL = "files"
	DUPL_COL  = "dupl"
	RC_COL    = "rc"
)

const (
	EntityNone EntityType = iota
	EntityGlusterFS
	EntityGridFS
	EntitySeadraFS
)

const (
	cqlSaveDupl          = `INSERT INTO dupl (id, domain, size, refid, cdate) VALUES (?, ?, ?, ?, ?)`
	cqlLookupDuplById    = `SELECT * FROM dupl WHERE id = ?`
	cqlLookupDuplByRefid = `SELECT * FROM dupl WHERE refid = ?`
	cqlRemoveDupl        = `DELETE FROM dupl WHERE id = ?`
	cqlLookupRefById     = `SELECT * FROM rc WHERE id = ?`
	cqlRemoveRef         = `DELETE FROM rc WHERE id = ?`
	cqlUpdateRefCnt      = `UPDATE rc SET refcnt = refcnt + %d WHERE id = ?`
	cqlLookupFileById    = `SELECT * FROM files WHERE id = ?`
	calLookupFileByMd5   = `SELECT * FROM md5 WHERE md5 = ? AND domain = ?`
	cqlSaveFile          = `INSERT INTO files (id, biz, cksize, domain, fn, size, md5, udate, uid, type, attrs) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	cqlRemoveFile        = `DELETE FROM files WHERE id = ?`

	cqlTouchHealth = `INSERT INTO health (id, magic) VALUES (?, ?)`
	cqlCheckHealth = `SELECT * FROM health WHERE id = ?`
)

// Type of storage which stores file content.
type EntityType uint8

// File represents the metadata of a DFS file.
type File struct {
	Id         string            `cql:"id"`
	Biz        string            `cql:"biz"`
	ChunkSize  int               `cql:"cksize"`
	Domain     int64             `cql:"domain"`
	Name       string            `cql:"fn"`
	Size       int64             `cql:"size"`
	Md5        string            `cql:"md5"`
	UploadDate time.Time         `cql:"udate"`
	UserId     string            `cql:"uid"`
	Metadata   map[string]string `cql:"attrs"`
	Type       EntityType        `cql:"type"`
}

// Dupl represents the metadata of a file duplication.
type Dupl struct {
	Id         string    `cql:"id"`
	Ref        string    `cql:"refid"`
	Length     int64     `cql:"size"`
	CreateDate time.Time `cql:"cdate"`
	Domain     int64     `cql:"domain"`
}

// Ref represents another metadata of a file duplication.
type Ref struct {
	Id     string `cql:"id"`
	RefCnt int64  `cql:"refcnt"`
}

type DraOpImpl struct {
	*gocql.ClusterConfig

	session *gocql.Session
	lock    sync.Mutex
}

// SaveDupl saves a dupl.
func (op *DraOpImpl) SaveDupl(dupl *Dupl) error {
	if len(dupl.Id) == 0 {
		dupl.Id = bson.NewObjectId().Hex()
	}
	if dupl.CreateDate.IsZero() {
		dupl.CreateDate = time.Now()
	}

	return op.execute(func(session *gocql.Session) error {
		b := cqlr.Bind(cqlSaveDupl, dupl)
		return b.Exec(session)
	})
}

// LookupDuplById looks up a dupl by its id.
func (op *DraOpImpl) LookupDuplById(id string) (*Dupl, error) {
	dupl := &Dupl{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupDuplById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(dupl)

		if len(dupl.Id) == 0 {
			dupl = nil
		}

		return nil
	})

	return dupl, err
}

// LookupDuplByRefid looks up a dupl by its ref id. No use right now.
func (op *DraOpImpl) LookupDuplByRefid(rid string) []*Dupl {
	result := make([]*Dupl, 0, 10)

	op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupDuplByRefid, rid)
		b := cqlr.BindQuery(q)
		defer b.Close()

		dupl := &Dupl{}
		for b.Scan(dupl) {
			if len(dupl.Id) == 0 {
				continue
			}
			result = append(result, dupl)
		}

		return nil
	})

	return result
}

// RemoveDupl removes a dupl by its id.
func (op *DraOpImpl) RemoveDupl(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveDupl, id).Exec()
	})
}

// SaveRef saves a reference.
func (op *DraOpImpl) SaveRef(ref *Ref) error {
	if len(ref.Id) == 0 {
		return errors.New("id of ref is nil.")
	}

	return op.addRefCnt(ref.Id, 0)
}

// LookupRefById looks up a ref by its id.
func (op *DraOpImpl) LookupRefById(id string) (*Ref, error) {
	ref := &Ref{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupRefById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(ref)

		if len(ref.Id) == 0 {
			ref = nil
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ref, nil
}

// RemoveRef removes a ref by its id.
func (op *DraOpImpl) RemoveRef(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveRef, id).Exec()
	})
}

// IncRefCnt increases reference count.
func (op *DraOpImpl) IncRefCnt(id string) (*Ref, error) {
	r, err := op.LookupRefById(id)
	if err != nil {
		return nil, err
	}

	err = op.addRefCnt(id, 1)
	if err != nil {
		return nil, err
	}

	if r != nil {
		r.RefCnt++
		return r, nil
	}

	return op.LookupRefById(id)
}

// DecRefCnt decreases reference count.
func (op *DraOpImpl) DecRefCnt(id string) (*Ref, error) {
	r, err := op.LookupRefById(id)
	if err != nil {
		return nil, err
	}

	err = op.addRefCnt(id, -1)
	if err != nil {
		return nil, err
	}

	if r != nil {
		r.RefCnt--
		return r, nil
	}

	return op.LookupRefById(id)
}

func (op *DraOpImpl) addRefCnt(id string, delta int) error {
	query := fmt.Sprintf(cqlUpdateRefCnt, delta)
	return op.execute(func(session *gocql.Session) error {
		return session.Query(query, id).Consistency(gocql.All).Exec()
	})
}

// LookupFileById looks up a file by its id.
func (op *DraOpImpl) LookupFileById(id string) (*File, error) {
	f := &File{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(cqlLookupFileById, id)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(f)

		if len(f.Id) == 0 {
			f = nil
			return meta.FileNotFound
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return f, nil
}

// LookupFileByMd5 looks up a file by its md5.
func (op *DraOpImpl) LookupFileByMd5(md5 string, domain int64) (*File, error) {
	f := &File{}
	err := op.execute(func(session *gocql.Session) error {
		q := session.Query(calLookupFileByMd5, md5, domain)
		b := cqlr.BindQuery(q)
		defer b.Close()

		if b.Scan(f) {
			return nil
		}

		return meta.FileNotFound
	})

	return f, err
}

// SaveFile saves a file.
func (op *DraOpImpl) SaveFile(f *File) error {
	if f.Type == EntityNone {
		return errors.New("File type unknown.")
	}

	return op.execute(func(session *gocql.Session) error {
		b := cqlr.Bind(cqlSaveFile, f)
		return b.Exec(session)
	})
}

// RemoveFile removes a file by its id.
func (op *DraOpImpl) RemoveFile(id string) error {
	return op.execute(func(session *gocql.Session) error {
		return session.Query(cqlRemoveFile, id).Exec()
	})
}

// Health checks the health of cql.
func (op *DraOpImpl) HealthCheck(node string) error {
	magic := time.Now().Unix()
	return op.execute(func(session *gocql.Session) error {
		err := session.Query(cqlTouchHealth, node, magic).Exec()
		if err != nil {
			return err
		}

		var health struct {
			Id    string `cql:"id"`
			Magic int64  `cql:"magic"`
		}
		q := session.Query(cqlCheckHealth, node)
		b := cqlr.BindQuery(q)
		defer b.Close()
		b.Scan(&health)
		if health.Magic != magic {
			return fmt.Errorf("Not the same number %d %d.", health.Magic, magic)
		}

		return nil
	})
}

// getSession returns a cql session.
// It makes dfs server can start before cassandra.
func (op *DraOpImpl) getSession() (*gocql.Session, error) {
	if op.session != nil {
		return op.session, nil
	}

	op.lock.Lock()
	defer op.lock.Unlock()

	if op.session != nil {
		return op.session, nil
	}

	var err error
	op.session, err = op.CreateSession()

	return op.session, err
}

func (op *DraOpImpl) execute(target func(session *gocql.Session) error) error {
	session, err := op.getSession()
	if err != nil {
		return err
	}

	return target(session)
}

// NewDraOpImpl returns a DraOpImpl object with given parameters.
func NewDraOpImpl(seeds []string, sqlOptions ...func(*DraOpImpl)) *DraOpImpl {
	// TODO(hanyh): Consider re-use letsgo/cql/conf to create cluster.
	self := &DraOpImpl{
		ClusterConfig: gocql.NewCluster(seeds...),
	}

	self.ClusterConfig.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

	for _, option := range sqlOptions {
		option(self)
	}

	return self
}

// ParseCqlOptions parses options of cassandra from map.
func ParseCqlOptions(attr map[string]interface{}) []func(*DraOpImpl) {
	options := make([]func(*DraOpImpl), 0, len(attr))

	options = append(options, func(m *DraOpImpl) {
		port, ok := attr[cqlAttrPort].(int)
		if !ok {
			return
		}
		m.Port = port
	})

	options = append(options, func(m *DraOpImpl) {
		timeout, ok := attr[cqlAttrTimeout].(int)
		if !ok {
			return
		}
		m.Timeout = time.Millisecond * time.Duration(timeout)
	})

	options = append(options, func(m *DraOpImpl) {
		conns, ok := attr[cqlAttrConns].(int)
		if !ok {
			return
		}
		m.NumConns = conns
	})

	options = append(options, func(m *DraOpImpl) {
		ks := attr[cqlAttrKeyspace].(string) // panic
		m.Keyspace = ks
	})

	options = append(options, func(m *DraOpImpl) {
		consistency, ok := attr[cqlAttrConsistency].(string)
		if !ok {
			return
		}
		m.Consistency = gocql.ParseConsistency(consistency)
	})

	options = append(options, func(m *DraOpImpl) {
		user, ok := attr[cqlAttrUser].(string)
		if !ok || len(user) == 0 {
			return
		}
		pass, ok := attr[cqlAttrPass].(string)
		if !ok {
			return
		}

		m.Authenticator = gocql.PasswordAuthenticator{
			Username: user,
			Password: pass,
		}
	})

	return options
}
