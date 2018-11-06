package metadata

import (
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func TestSaveSpaceLog(t *testing.T) {
	lop, err := NewSpaceLogOp(dbName, dbUri)
	if err != nil {
		t.Errorf("NewSpaceLogOp error %v", err)
	}

	log1 := &SpaceLog{
		Domain: 2,
		Uid:    "1001",
		Fid:    bson.NewObjectId().Hex(),
		Size:   2345,
		Type:   CreateType.String(),
	}
	lop.SaveSpaceLog(log1)

	log2 := &SpaceLog{
		Id:        bson.NewObjectId(),
		Domain:    2,
		Uid:       "1001",
		Fid:       bson.NewObjectId().Hex(),
		Biz:       "unit-test",
		Size:      2345,
		Timestamp: time.Now(),
		Type:      CreateType.String(),
	}
	lop.SaveSpaceLog(log2)

	log3 := &SpaceLog{
		Id:        bson.NewObjectId(),
		Domain:    2,
		Uid:       "1001",
		Fid:       bson.NewObjectId().Hex(),
		Biz:       "unit-test",
		Size:      2345,
		Timestamp: time.Now(),
		Type:      DeleteType.String(),
	}
	lop.SaveSpaceLog(log3)
}
