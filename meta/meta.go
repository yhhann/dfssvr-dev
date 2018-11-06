package meta

import (
	"errors"
	"time"
)

const (
	EntityNone EntityType = iota
	EntityGlusterFS
	EntityGridFS
	EntitySeaweedFS
)

// Type of file entity.
type EntityType uint8

// File represents the metadata of a file.
type File struct {
	Id         string
	Biz        string
	Name       string
	Md5        string
	UserId     string
	Domain     int64
	Size       int64
	ChunkSize  int
	UploadDate time.Time
	ExtAttr    map[string]string
	Type       EntityType
}

type FileMetaOp interface {
	// Find looks up the metadata of a file by its fid.
	Find(fid string) (*File, error)

	// Save saves the metadata of a file.
	Save(*File) error

	// FindByMd5 looks up the metadata of a file by its md5 and domain.
	FindByMd5(md5 string, domain int64) (*File, error)

	// DuplicateWithId duplicates a given file by its fid.
	DuplicateWithId(fid string, did string, createDate time.Time) (string, error)

	// Delete deletes a file.
	Delete(fid string) (tobeDeleted bool, entityIdToBeDeleted string, err error)
}

var (
	FileNotFound      = errors.New("file not found")
	FileAlreadyExists = errors.New("file already exists")
)
