package metadata

import (
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	DUPL_COL = "dupl"
	RC_COL   = "rc"
)

type Dupl struct {
	Id         bson.ObjectId `bson:"_id"`              // id
	Ref        bson.ObjectId `bson:"reference"`        // reference
	Length     int64         `bson:"length"`           // length
	UploadDate time.Time     `bson:"uploadDate"`       // upload date
	Domain     int64         `bson:"domain,omitempty"` // domain
}

type Ref struct {
	Id         bson.ObjectId `bson:"_id"`        // id
	RefCnt     int64         `bson:"refcnt"`     // referenc count
	Length     int64         `bson:"length"`     // length
	UploadDate time.Time     `bson:"uploadDate"` // upload date
}

type DuplicateOp struct {
	dbName string
	uri    string

	prefix      string
	duplColName string
	rcColName   string
}

func (d *DuplicateOp) execute(target func(session *mgo.Session) error) error {
	s, err := CopySession(d.uri)
	if err != nil {
		return err
	}
	defer ReleaseSession(s)

	return target(s)
}

func (d *DuplicateOp) Close() {
}

// SaveDupl saves a dupl.
func (d *DuplicateOp) SaveDupl(dupl *Dupl) error {
	if string(dupl.Id) == "" {
		dupl.Id = bson.NewObjectId()
	}
	if !dupl.Id.Valid() {
		return ObjectIdInvalidError
	}
	if dupl.UploadDate.IsZero() {
		dupl.UploadDate = time.Now()
	}

	return d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.duplColName).Insert(*dupl)
	})
}

// SaveRef saves a reference.
func (d *DuplicateOp) SaveRef(ref *Ref) error {
	if string(ref.Id) == "" {
		ref.Id = bson.NewObjectId()
	}
	if !ref.Id.Valid() {
		return ObjectIdInvalidError
	}
	if ref.UploadDate.IsZero() {
		ref.UploadDate = time.Now()
	}

	return d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.rcColName).Insert(*ref)
	})
}

// LookupRefById looks up a ref by its id.
func (d *DuplicateOp) LookupRefById(id bson.ObjectId) (*Ref, error) {
	ref := new(Ref)
	err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.rcColName).FindId(id).One(ref)
	})
	if err == mgo.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return ref, nil
}

// LookupDuplById looks up a dupl by its id.
func (d *DuplicateOp) LookupDuplById(id bson.ObjectId) (*Dupl, error) {
	dupl := new(Dupl)
	err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.duplColName).FindId(id).One(dupl)
	})
	if err == mgo.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return dupl, nil
}

// LookupDuplByRefid looks up a dupl by its ref id.
func (d *DuplicateOp) LookupDuplByRefid(rid bson.ObjectId) []*Dupl {
	result := make([]*Dupl, 0, 10)

	d.execute(func(session *mgo.Session) error {
		iter := session.DB(d.dbName).C(d.duplColName).Find(bson.M{"reference": rid}).Iter()
		defer iter.Close()

		for dupl := new(Dupl); iter.Next(dupl); dupl = new(Dupl) {
			result = append(result, dupl)
		}

		return nil
	})

	return result
}

// RemoveDupl removes a dupl by its id.
func (d *DuplicateOp) RemoveDupl(id bson.ObjectId) error {
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.duplColName).RemoveId(id)
	}); err != nil {
		return err
	}

	return nil
}

// RemoveRef removes a ref by its id.
func (d *DuplicateOp) RemoveRef(id bson.ObjectId) error {
	if err := d.execute(func(session *mgo.Session) error {
		return session.DB(d.dbName).C(d.rcColName).RemoveId(id)
	}); err != nil {
		return err
	}

	return nil
}

// IncRefCnt increases reference count.
func (d *DuplicateOp) IncRefCnt(id bson.ObjectId) (*Ref, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"refcnt": 1,
			},
		},
		ReturnNew: true,
	}

	result := new(Ref)
	if err := d.execute(func(session *mgo.Session) error {
		_, err := session.DB(d.dbName).C(d.rcColName).Find(bson.M{"_id": id}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// DecRefCnt decreases reference count.
func (d *DuplicateOp) DecRefCnt(id bson.ObjectId) (*Ref, error) {
	change := mgo.Change{
		Update: bson.M{
			"$inc": bson.M{
				"refcnt": -1,
			},
		},
		ReturnNew: true,
	}

	result := new(Ref)
	if err := d.execute(func(session *mgo.Session) error {
		_, err := session.DB(d.dbName).C(d.rcColName).Find(bson.M{"_id": id}).Apply(change, result)
		return err
	}); err != nil {
		return nil, err
	}

	return result, nil
}

// NewDuplicateOp creates a DuplicateOp object with given session
// and database name.
func NewDuplicateOp(dbName string, uri string, prefix string) (*DuplicateOp, error) {
	return &DuplicateOp{
		uri:         uri,
		dbName:      dbName,
		prefix:      prefix,
		duplColName: strings.Join([]string{prefix, DUPL_COL}, "."),
		rcColName:   strings.Join([]string{prefix, RC_COL}, "."),
	}, nil
}
