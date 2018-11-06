// Package cassandra renders a new type shard for file store.
// The new type shard stores file contents in glusterfs, and
// stores file metadatas in cassandra.

package cassandra

// Usage of unit test.
// To run unit test of this package:
// go test -v jingoal.com/dfs/cassandra -run ^TestDra.*$
