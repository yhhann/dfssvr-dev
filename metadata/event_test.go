package metadata

import (
	"fmt"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func TestEvent(t *testing.T) {
	sop, err := NewEventOp(dbName, dbUri)
	if err != nil {
		t.Errorf("init event col error %v\n", err)
	}

	id := bson.NewObjectId()
	e := Event{Id: id, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: "event-test", ThreadId: "thread-test", Description: "delete", Domain: -1}

	if err := sop.SaveEvent(&e); err != nil {
		t.Errorf("save event error %v\n", err)
	}

	ef, _ := sop.LookupEventById(id)
	if ef.Id != e.Id {
		t.Errorf("find event error %v\n", err)
	}

	if err := sop.RemoveEvent(id); err != nil {
		t.Errorf("remove event error %v\n", err)
	}
}

func TestGetEvents(t *testing.T) {
	fid := fmt.Sprintf("%x", string(bson.NewObjectId()))
	sop, err := NewEventOp(dbName, dbUri)
	if err != nil {
		t.Errorf("NewEventOp() error %v\n", err)
	}

	start := time.Now().Unix()
	time.Sleep(1 * time.Second)

	id1 := bson.NewObjectId()
	e1 := Event{Id: id1, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "create", Domain: -1}
	sop.SaveEvent(&e1)

	id2 := bson.NewObjectId()
	e2 := Event{Id: id2, Type: "testType", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "read", Domain: -1}
	sop.SaveEvent(&e2)

	id3 := bson.NewObjectId()
	e3 := Event{Id: id3, Type: "testType1", Timestamp: time.Now().Unix(),
		EventId: fid, ThreadId: "2000", Description: "delete", Domain: -1}
	sop.SaveEvent(&e3)

	time.Sleep(1 * time.Second)
	end := time.Now().Unix()

	var e Event
	cnt := 0
	iter := sop.GetEvents("", "", start, end)
	for iter.Next(&e) {
		cnt++
	}
	iter.Close()
	if cnt != 3 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 3 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType", "", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 2 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType1", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 1 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	cnt = 0
	iter = sop.GetEvents("testType", "2000", start, end)
	for iter.Next(&e) {
		cnt++
	}
	if cnt != 2 {
		t.Errorf("GetEvent error, %d", cnt)
	}

	sop.RemoveEvent(id1)
	sop.RemoveEvent(id2)
	sop.RemoveEvent(id3)
}
