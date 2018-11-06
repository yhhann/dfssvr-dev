package fileop

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func lazyRemove(gridfs *mgo.GridFS, id bson.ObjectId) error {
	// TODO(hanyh): lazy remove
	return nil
}

func removeEntity(gridfs *mgo.GridFS, id bson.ObjectId) error {
	return gridfs.RemoveId(id)
}
