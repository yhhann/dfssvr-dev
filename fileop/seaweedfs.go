package fileop

import (
	"fmt"
	"time"

	"jingoal.com/seaweedfs-adaptor/weedfs"
)

const (
	MetaKey_WeedFid   = "weedfid"
	MetaKey_Chunksize = "chunksize"

	DefaultSeaweedChunkSize = 512 * 1024
)

func SeaweedFSHealthCheck(master, replica, datacenter, rack, checkId string) error {
	domain := int64(2)
	content := fmt.Sprintf("%s:%d", checkId, time.Now().Unix())
	wFile, err := weedfs.Create("health-check-file", domain, master, replica, datacenter, rack, 0)
	if err != nil {
		return err
	}
	defer wFile.Close()

	if _, err := wFile.Write([]byte(content)); err != nil {
		return err
	}
	if _, err := weedfs.Remove(wFile.Fid, domain, master); err != nil {
		return err
	}

	return nil
}
