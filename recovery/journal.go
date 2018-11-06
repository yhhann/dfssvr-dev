package recovery

import (
	"fmt"

	"gopkg.in/mgo.v2/bson"
)

type RecoveryEventType uint

const (
	NewEvent RecoveryEventType = iota
	TreatedEvent
)

// RecoveryEvent represents an event of degradation.
type RecoveryEvent struct {
	Id        bson.ObjectId     `bson:"_id"`
	Fid       string            `bson:"fid"`
	Domain    int64             `bson:"domain"`
	Timestamp int64             `bson:"tm"`
	Type      RecoveryEventType `bson:"type,omitempty"`
}

func (e *RecoveryEvent) String() string {
	return fmt.Sprintf("fid: %s, domain: %d, timestamp: %d, type: %d",
		e.Fid, e.Domain, e.Timestamp, e.Type)
}
