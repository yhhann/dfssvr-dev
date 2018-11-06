package metadata

import (
	"errors"
	"flag"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	SEGMENT_COL = "chunks"  // segment collection name
	SHARD_COL   = "servers" // shard collection name
)

var (
	MongoTimeout = flag.Int("mongo-timeout", 100, "mongdb connection timeout")

	ObjectIdInvalidError = errors.New("ObjectId invalid")
)

// MongoMetaOp implements MetaOp.
type MongoMetaOp struct {
	uri    string
	dbName string
}

func (op *MongoMetaOp) execute(target func(session *mgo.Session) error) error {
	s, err := CopySession(op.uri)
	if err != nil {
		return err
	}
	defer ReleaseSession(s)

	return target(s)
}

func (op *MongoMetaOp) Close() {
}

// SaveSegment saves a Segment object into "chunks" collection.
// If id of the saved object is nil, it will be set to a new ObjectId.
func (op *MongoMetaOp) SaveSegment(seg *Segment) error {
	if string(seg.Id) == "" {
		seg.Id = bson.NewObjectId()
	}
	if !seg.Id.Valid() {
		return ObjectIdInvalidError
	}

	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SEGMENT_COL).Insert(seg)
	})
}

// UpdateSegment updates a segment object.
func (op *MongoMetaOp) UpdateSegment(seg *Segment) error {
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SEGMENT_COL).Update(
			bson.M{"domain": seg.Domain},
			bson.M{"$set": bson.M{"normalServer": seg.NormalServer, "migrateServer": seg.MigrateServer}})
	})
}

// LookupSegmentByDomain finds a segment by given domain.
func (op *MongoMetaOp) LookupSegmentByDomain(domain int64) (*Segment, error) {
	seg := new(Segment)
	err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SEGMENT_COL).Find(bson.M{"domain": domain}).One(seg)
	})

	if err != nil {
		return nil, err
	}
	return seg, nil
}

// FindNextDomainSegment finds the first segment whose domain greater than
// the given domain.
func (op *MongoMetaOp) FindNextDomainSegment(domain int64) (*Segment, error) {
	seg := new(Segment)

	if err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SEGMENT_COL).Find(bson.M{"domain": bson.M{"$gt": domain}}).One(seg)
	}); err != nil {
		return nil, err
	}

	return seg, nil
}

// RemoveSegment removes a segment by its domain.
func (op *MongoMetaOp) RemoveSegment(domain int64) error {
	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SEGMENT_COL).Remove(bson.M{"domain": domain})
	})
}

// FindAllSegmentsOrderByDomain finds all segment from collection "chunks"
func (op *MongoMetaOp) FindAllSegmentsOrderByDomain() []*Segment {
	result := make([]*Segment, 0, 100)

	op.execute(func(session *mgo.Session) error {
		iter := session.DB(op.dbName).C(SEGMENT_COL).Find(nil).Sort("domain").Iter()
		defer iter.Close()

		for seg := new(Segment); iter.Next(seg); seg = new(Segment) {
			result = append(result, seg)
		}

		return nil
	})

	return result
}

// LookupShardByName finds a shard server by its name.
func (op *MongoMetaOp) LookupShardByName(name string) (*Shard, error) {
	s := new(Shard)
	if err := op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(SHARD_COL).Find(bson.M{"name": name}).One(s)
	}); err != nil {
		return nil, err
	}
	return s, nil
}

// FindAllShards finds all shard servers.
func (op *MongoMetaOp) FindAllShards() []*Shard {
	result := make([]*Shard, 0, 10)
	op.execute(func(session *mgo.Session) error {
		iter := session.DB(op.dbName).C(SHARD_COL).Find(bson.M{}).Sort("name").Iter()
		defer iter.Close()

		for s := new(Shard); iter.Next(s); s = new(Shard) {
			result = append(result, s)
		}

		return nil
	})

	return result
}

// NewMongoMetaOp creates a MongoMetaOp object with given mongodb uri
// and database name.
func NewMongoMetaOp(dbName string, uri string) (*MongoMetaOp, error) {
	return &MongoMetaOp{
		uri:    uri,
		dbName: dbName,
	}, nil
}
