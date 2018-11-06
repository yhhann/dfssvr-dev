package metadata

// MetaOp represents the operator of metadata.
type MetaOp interface {
	// Segment operators

	// SaveSegment saves a segment into database.
	SaveSegment(c *Segment) error

	// UpdateSegment updates segment.
	UpdateSegment(c *Segment) error

	// LookupSegmentByDomain finds a segment.
	LookupSegmentByDomain(domain int64) (*Segment, error)

	// FindNextDomainSegment finds the first segment
	// its domain greater than the given domain.
	FindNextDomainSegment(domain int64) (*Segment, error)

	// RemoveSegment removes a segment.
	RemoveSegment(domain int64) error

	// FindAllSegmentOrderByDomain finds all segment from database.
	FindAllSegmentsOrderByDomain() []*Segment

	// Shard operators

	// LookupShardByName finds a shard server by its name.
	LookupShardByName(name string) (*Shard, error)

	// FindAllShards finds all shard servers.
	FindAllShards() []*Shard

	// Close releases session hold by MetaOp.
	Close()
}
