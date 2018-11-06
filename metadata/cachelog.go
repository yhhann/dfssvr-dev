package metadata

import (
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	CACHELOG_COL = "cachelog" // cachelog collection name

	CACHELOG_STATE_PENDING     = 0
	CACHELOG_STATE_PROCESSING  = 1
	CACHELOG_STATE_SRC_DAMAGED = 2
	CACHELOG_STATE_FINISHED    = 3
)

// CacheLog represents an cache log.
type CacheLog struct {
	Id         bson.ObjectId `bson:"_id"`        // id, same as fid
	Timestamp  int64         `bson:"timestamp"`  // timestamp
	Domain     int64         `bson:"domain"`     // domain
	Fid        string        `bson:"fid"`        // fid
	RetryTimes int64         `bson:"retrytimes"` // retry times
	State      int64         `bson:"state"`      // state
	Shard      string        `bson:"shard"`      // shard
	Cause      string        `bson:"cause"`      // cause

	CacheId        string `bson:"cachefid"`    // cached fid
	CacheChunkSize int64  `bson:"cachecksize"` // cached chunk size
}

// String returns a string for CacheLog
func (g *CacheLog) String() string {
	return fmt.Sprintf("CacheLog[Fid %s, CacheId %s, Domain %d, ReTry %d, State %d, Shard %s, Cause '%s', %s]",
		g.Fid, g.CacheId, g.Domain, g.RetryTimes, g.State, g.Shard, g.Cause, time.Unix(int64(g.Timestamp), 0).Format("2006-01-02 15:04:05"))
}

type CacheLogIter struct {
	*mgo.Iter
	session *mgo.Session
}

func (i *CacheLogIter) Close() {
	if i.Iter != nil {
		i.Iter.Close()
	}
	if i.session != nil {
		ReleaseSession(i.session)
	}
}

type CacheLogOp struct {
	uri    string
	dbName string
}

func (op *CacheLogOp) execute(target func(session *mgo.Session) error) error {
	s, err := CopySession(op.uri)
	if err != nil {
		return err
	}
	defer ReleaseSession(s)

	return target(s)
}

func (op *CacheLogOp) Close() {
}

// LookupCacheLogById finds an cache log by its id.
func (op *CacheLogOp) LookupCacheLogById(id bson.ObjectId) (*CacheLog, error) {
	log := &CacheLog{}
	if err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(CACHELOG_COL).FindId(id).One(log)
	}); err != nil {
		return nil, err
	}

	return log, nil
}

// GetCacheLogs gets an cache log iterator.
func (op *CacheLogOp) GetCacheLogs(limit int) (*CacheLogIter, error) {
	s, err := CopySession(op.uri)
	if err != nil {
		return nil, err
	}

	q := bson.M{"state": CACHELOG_STATE_PENDING}
	iter := s.DB(op.dbName).C(CACHELOG_COL).Find(q).Limit(limit).Iter()

	return &CacheLogIter{
		Iter:    iter,
		session: s,
	}, nil
}

// SaveOrUpdate saves cache log if not exists, or updates it if exists.
func (op *CacheLogOp) SaveOrUpdate(log *CacheLog) (*CacheLog, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"retrytimes": 1,
			},
			"$set": bson.M{
				"fid":         log.Fid,
				"domain":      log.Domain,
				"timestamp":   log.Timestamp,
				"state":       log.State,
				"shard":       log.Shard,
				"cachefid":    log.CacheId,
				"cachecksize": log.CacheChunkSize,
				"cause":       log.Cause,
			},
		},
		Upsert:    true,
		ReturnNew: true,
	}

	result := &CacheLog{}
	if err := op.execute(func(session *mgo.Session) error {
		_, err := session.DB(op.dbName).C(CACHELOG_COL).Find(bson.M{"fid": log.Fid, "domain": log.Domain}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// GetFinishedCacheLogByTime gets cache logs by it's timestamp.
func (op *CacheLogOp) GetFinishedCacheLogByTime(timestamp int64) ([]CacheLog, error) {
	var result []CacheLog
	err := op.execute(func(session *mgo.Session) error {
		q := bson.M{"state": CACHELOG_STATE_FINISHED, "timestamp": bson.M{"$lte": timestamp}}
		return session.DB(op.dbName).C(CACHELOG_COL).Find(q).All(result) // limit ?
	})

	return result, err
}

// RemoveFinishedCacheLogByTime removes cache logs by it's timestamp.
func (op *CacheLogOp) RemoveFinishedCacheLogByTime(timestamp int64) error {
	return op.execute(func(session *mgo.Session) error {
		q := bson.M{"state": CACHELOG_STATE_FINISHED, "timestamp": bson.M{"$lte": timestamp}}
		_, err := session.DB(op.dbName).C(CACHELOG_COL).RemoveAll(q)
		return err
	})
}

// RemoveCacheLogById removes an cache log by its id.
func (op *CacheLogOp) RemoveCacheLogById(id bson.ObjectId) error {
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(CACHELOG_COL).RemoveId(id)
	})
}

// NewCacheLogOp creates an CacheLogOp object with given mongodb uri
// and database name.
func NewCacheLogOp(dbName string, uri string) (*CacheLogOp, error) {
	return &CacheLogOp{
		uri:    uri,
		dbName: dbName,
	}, nil
}
