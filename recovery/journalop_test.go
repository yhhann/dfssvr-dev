package recovery

import (
	"fmt"
	"testing"
)

const (
	dbUri = "mongodb://192.168.55.193:27017?maxPoolSize=30"
	cnt   = 100000
)

func createEvents(dop *RecoveryEventOp, count int, prefix string) error {
	for i := 1; i <= count; i++ {
		err := dop.SaveEvent(&RecoveryEvent{
			Domain:    1,
			Timestamp: 1000 + int64(i),
			Fid:       fmt.Sprintf("%s%d", prefix, i),
		})
		if err != nil {
			return err
		}

		if i%1000 == 0 {
			fmt.Printf("Event created: %d\n", i)
		}
	}

	return nil
}

func TestNormal(t *testing.T) {
	dop, err := NewRecoveryEventOp("event", dbUri)
	if err != nil {
		t.Errorf("Init degradation event col error %v", err)
	}

	createEvents(dop, cnt, "first")
	events, err := dop.GetEventsInBatch(cnt, 3000)
	if err != nil {
		t.Errorf("GetEvents error %v", err)
	}

	if len(events) != cnt {
		t.Errorf("GetEvents count error %d\n", len(events))
	}

	for _, e := range events {
		dop.RemoveEvent(e.Id)
	}
}

func TestTimeout1(t *testing.T) {
	dop, err := NewRecoveryEventOp("event", dbUri)
	if err != nil {
		t.Errorf("Init degradation event col error %v", err)
	}

	createEvents(dop, cnt, "first")
	events, err := dop.GetEventsInBatch(cnt*10, 1)
	if err != nil {
		t.Errorf("GetEvents error %v", err)
	}

	if len(events) == cnt {
		t.Errorf("GetEvents count error %d\n", len(events))
	}

	for _, e := range events {
		dop.RemoveEvent(e.Id)
	}
}

func TestTimeout2(t *testing.T) {
	dop, err := NewRecoveryEventOp("event", dbUri)
	if err != nil {
		t.Errorf("Init degradation event col error %v", err)
	}

	createEvents(dop, cnt, "first")
	events, err := dop.GetEventsInBatch(cnt*10, cnt*15)
	if err != nil {
		t.Errorf("GetEvents error %v", err)
	}

	if len(events) != cnt {
		t.Errorf("GetEvents count error %d\n", len(events))
	}

	for _, e := range events {
		dop.RemoveEvent(e.Id)
	}
}
