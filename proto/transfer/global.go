package transfer

import (
	"strconv"

	"github.com/golang/glog"

	"jingoal.com/dfs/meta"
)

var (
	// NodeName represents name of this node.
	NodeName string

	// ServerId represents id of this server.
	ServerId string
)

func File2Info(f *meta.File) *FileInfo {
	userId, err := strconv.ParseInt(f.UserId, 10, 64)
	if err != nil {
		glog.Warningf("Invalid user id %s, %v.", f.UserId, err)
	}

	return &FileInfo{
		Id:     f.Id,
		Name:   f.Name,
		Md5:    f.Md5,
		Biz:    f.Biz,
		Size:   f.Size,
		Domain: f.Domain,
		User:   userId,
	}
}
