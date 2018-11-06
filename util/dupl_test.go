package util

import "testing"

var (
	duplId = "_abcdefg"
	realId = "abcdefg"
)

func TestIsDuplId(t *testing.T) {
	if !IsDuplId(duplId) {
		t.Errorf("IsDuplId error %s", duplId)
	}

	if IsDuplId(realId) {
		t.Errorf("IsDuplId error %s", realId)
	}
}

func TestGetRealId(t *testing.T) {
	rId1 := GetRealId(duplId)
	if rId1 != realId {
		t.Errorf("GetRealId error %s", duplId)
	}

	rId2 := GetRealId(realId)
	if rId2 != realId {
		t.Errorf("GetRealId error %s", realId)
	}
}

func TestGetDuplId(t *testing.T) {
	dId1 := GetDuplId(duplId)
	if dId1 != duplId {
		t.Errorf("GetDuplId error %s", duplId)
	}

	dId2 := GetDuplId(realId)
	if dId2 != duplId {
		t.Errorf("GetDuplId error %s", realId)
	}
}
