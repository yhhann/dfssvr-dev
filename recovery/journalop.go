package recovery

import (
	"sync/atomic"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/metadata"
)

const (
	DEGRADATION_COL = "degradation" // degradation event collection name
)

// RecoveryEventOp processes biz logic about degradation event.
type RecoveryEventOp struct {
	uri    string
	dbName string
}

func (op *RecoveryEventOp) execute(target func(session *mgo.Session) error) error {
	s, err := metadata.CopySession(op.uri)
	if err != nil {
		return err
	}
	defer metadata.ReleaseSession(s)

	return target(s)
}

func (op *RecoveryEventOp) Close() {
}

// SaveEvent saves a degradation event.
func (op *RecoveryEventOp) SaveEvent(e *RecoveryEvent) error {
	e.Id = bson.NewObjectId()

	return op.execute(func(session *mgo.Session) error {
		return session.DB(op.dbName).C(DEGRADATION_COL).Insert(e)
	})
}

// RemoveEvent removes a degradation event.
func (op *RecoveryEventOp) RemoveEvent(id bson.ObjectId) error {
	return op.execute(func(s *mgo.Session) error {
		return s.DB(op.dbName).C(DEGRADATION_COL).RemoveId(id)
	})
}

// GetEventsByBatch gets at most batch RecoveryEvents within timeout milliseconds.
func (op *RecoveryEventOp) GetEventsInBatch(batch int, timeout int64) ([]*RecoveryEvent, error) {
	result := make([]*RecoveryEvent, 0, batch)

	var iter *mgo.Iter
	op.execute(func(session *mgo.Session) error {
		iter = session.DB(op.dbName).C(DEGRADATION_COL).Find(nil).Limit(batch).Sort("_id").Iter()
		defer iter.Close()

		// 0 stands for not timeout, 1 stands for timeout.
		var isTimeout int32

		go func() {
			ticker := time.NewTicker(time.Duration(timeout) * time.Millisecond)
			defer ticker.Stop()

			select {
			case <-ticker.C:
				atomic.StoreInt32(&isTimeout, 1)
			}
		}()

		for re := new(RecoveryEvent); iter.Next(re); re = new(RecoveryEvent) {
			result = append(result, re)
			if atomic.LoadInt32(&isTimeout) == 1 {
				break
			}
		}

		return nil
	})

	return result, nil
}

// NewRecoveryEventOp creates a RecoveryEventOp
// with given mongodb uri and database name.
func NewRecoveryEventOp(dbName string, uri string) (*RecoveryEventOp, error) {
	return &RecoveryEventOp{
		uri:    uri,
		dbName: dbName,
	}, nil
}
