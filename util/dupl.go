package util

import "strings"

const (
	duplPrefix = '_'
)

func IsDuplId(id string) bool {
	if id[0] == duplPrefix {
		return true
	}

	return false
}

func GetDuplId(id string) string {
	if IsDuplId(id) {
		return id
	}

	return strings.Join([]string{string(duplPrefix), id}, "")
}

func GetRealId(id string) string {
	if IsDuplId(id) {
		return id[1:]
	}

	return id
}
