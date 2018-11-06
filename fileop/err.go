package fileop

import (
	"fmt"
)

const (
	_ = iota
	GlusterFSFileError
)

var (
	NoEntityError = fmt.Errorf("no entity")
)

type RecoverableFileError struct {
	Code int
	Orig error
}

func (e RecoverableFileError) Error() string {
	return fmt.Sprintf("recoverable file error, code %d, %v", e.Code, e.Orig)
}
