package cassandra

// Draop processes the operation against cassandra.
type DraOp interface {
	// LookupFileById looks up a file by its id.
	LookupFileById(id string) (*File, error)

	// LookupFileByMd5 looks up a file by its md5.
	LookupFileByMd5(md5 string, domain int64) (*File, error)

	// SaveFile saves a file.
	SaveFile(f *File) error

	// RemoveFile removes a file by its id.
	RemoveFile(id string) error

	// SaveDupl saves a dupl.
	SaveDupl(dupl *Dupl) error

	// LookupDuplById looks up a dupl by its id.
	LookupDuplById(id string) (*Dupl, error)

	// LookupDuplByRefid looks up a dupl by its ref id. No use right now.
	LookupDuplByRefid(rid string) []*Dupl

	// RemoveDupl removes a dupl by its id.
	RemoveDupl(id string) error

	// SaveRef saves a reference.
	SaveRef(ref *Ref) error

	// LookupRefById looks up a ref by its id.
	LookupRefById(id string) (*Ref, error)

	// RemoveRef removes a ref by its id.
	RemoveRef(id string) error

	// IncRefCnt increases reference count.
	IncRefCnt(id string) (*Ref, error)

	// DecRefCnt decreases reference count.
	DecRefCnt(id string) (*Ref, error)

	// HealthCheck checks the health of cassandra.
	HealthCheck(node string) error
}
