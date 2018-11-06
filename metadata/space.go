package metadata

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/instrument"
)

const (
	CreateType SpaceLogType = iota
	DeleteType
)

// SpaceLogType represents the type of space log.
type SpaceLogType uint

func (t SpaceLogType) String() string {
	if t == CreateType {
		return "create"
	}

	return "delete"
}

func NewSpaceLogType(v string) SpaceLogType {
	if v == "create" {
		return CreateType
	}

	return DeleteType
}

// SpaceLog represents space log in db.
type SpaceLog struct {
	Id        bson.ObjectId `bson:"_id"`       // id
	Domain    int64         `bson:"domain"`    // domain
	Uid       string        `bson:"uid"`       // User id
	Fid       string        `bson:"fid"`       // File id
	Biz       string        `bson:"biz"`       // Biz
	Size      int64         `bson:"size"`      // length
	Timestamp time.Time     `bson:"timestamp"` // timestamp
	Type      string        `bson:"type"`      // type
}

// String returns a string for logging into file.
func (s *SpaceLog) String() string {
	return fmt.Sprintf("SpaceLog[Domain %d, Uid %s, Fid %s, Biz %s, Size %d, Timestamp %v, Type %s]",
		s.Domain, s.Uid, s.Fid, s.Biz, s.Size, s.Timestamp, s.Type)
}

type SpaceLogOp struct {
	uri    string
	dbName string
}

func (op *SpaceLogOp) execute(target func(*mgo.Session) error) error {
	s, err := CopySession(op.uri)
	if err != nil {
		return err
	}
	defer ReleaseSession(s)

	return target(s)
}

func (op *SpaceLogOp) Close() {
}

func (op *SpaceLogOp) SaveSpaceLog(log *SpaceLog) error {
	if *asyncEvent {
		go func() {
			instrument.AsyncSaving <- &instrument.Measurements{
				Name:  "slog",
				Value: 1,
			}
			defer func() {
				instrument.AsyncSaving <- &instrument.Measurements{
					Name:  "slog",
					Value: -1,
				}
			}()

			if err := op.saveSpaceLog(log); err != nil {
				glog.Warningf("%s, error: %v", log.String(), err)
			}
		}()

		return nil
	}

	return op.saveSpaceLog(log)
}

func (op *SpaceLogOp) saveSpaceLog(log *SpaceLog) error {
	if string(log.Id) == "" {
		log.Id = bson.NewObjectId()
	}
	if !log.Id.Valid() {
		return ObjectIdInvalidError
	}
	if log.Timestamp.IsZero() {
		log.Timestamp = time.Now()
	}
	if strings.TrimSpace(log.Biz) == "" {
		log.Biz = "general"
	}

	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C("slog").Insert(*log)
	})
}

// NewSpaceLogOp creates a SpaceLogOp object with given mongodb uri
// and database name.
func NewSpaceLogOp(dbName string, uri string) (*SpaceLogOp, error) {
	return &SpaceLogOp{
		uri:    uri,
		dbName: dbName,
	}, nil
}
