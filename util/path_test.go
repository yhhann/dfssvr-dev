package util

import (
	"testing"
)

func TestSanitizeDigit(t *testing.T) {
	if sanitizeDigit(2) != 2 {
		t.Error("sanitizeDigit(2) != 2")
	}
	if sanitizeDigit(3) != 3 {
		t.Error("sanitizeDigit(3) != 3")
	}
	if sanitizeDigit(4) != 4 {
		t.Error("sanitizeDigit(4) != 4")
	}
	if sanitizeDigit(1) != 2 {
		t.Error("sanitizeDigit(1) != 2")
	}
	if sanitizeDigit(5) != 4 {
		t.Error("sanitizeDigit(5) != 2")
	}
}

func TestGetZeros(t *testing.T) {
	if "00" != getZeros(2) {
		t.Error("getDummy(2) != 00")
	}
	if "000" != getZeros(3) {
		t.Error("getDummy(3) != 000")
	}
	if "0000" != getZeros(4) {
		t.Error("getDummy(4) != 0000")
	}
	if "00" != getZeros(0) {
		t.Error("getDummy(0) != 00")
	}
}

func TestGetPathLevel3(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	expected := "/mnt/base/g3456/123456/56/564edc6a65d7caed29056b5c"
	result := getPathLevel3("/mnt/base", 123456, fn, 2)
	if result != expected {
		t.Errorf("getPathLevel3() return not expected: %q", result)
	}
}

func TestGetPathLevel4(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	expected := "/mnt/base/g3456/123456/56/4e/564edc6a65d7caed29056b5c"
	result := getPathLevel4("/mnt/base", 123456, fn, 2)
	if result != expected {
		t.Errorf("getPathLevel4() return not expected: %q", result)
	}
}

func TestGetPathLevel5(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	expected := "/mnt/base/g3456/123456/56/4e/dc/564edc6a65d7caed29056b5c"
	result := getPathLevel5("/mnt/base", 123456, fn, 2)
	if result != expected {
		t.Errorf("getPathLevel5() return not expected: %q", result)
	}
}

func TestGetPathLevel6(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	expected := "/mnt/base/g3456/123456/56/4e/dc/6a/564edc6a65d7caed29056b5c"
	result := getPathLevel6("/mnt/base", 123456, fn, 2)
	if result != expected {
		t.Errorf("getPathLevel6() return not expected: %q", result)
	}
}

func TestGetOldFilePath(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	expected := "/mnt/base/123456/" + fn
	result := getOldFilePath("/mnt/base", 123456, fn)
	if result != expected {
		t.Errorf("getOldFilePath() return not expected: %q", result)
	}
}

func TestGetFilePath(t *testing.T) {
	fn := "564edc6a65d7caed29056b5c"
	result := GetFilePath("", 123456, fn, PathLevel4, 2)
	expected := "g3456/123456/56/4e/564edc6a65d7caed29056b5c"

	if result != expected {
		t.Errorf("GetFilePath() return not expected: %q", result)
	}
}
