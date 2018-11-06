package util

import (
	"runtime/debug"
	"time"
)

func GetTimeInMilliSecond() int64 {
	return time.Now().UnixNano() / 1e6
}

func PrintStack() {
	debug.PrintStack()
}
