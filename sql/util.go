package sql

import (
	"encoding/json"

	"jingoal.com/dfs/meta"
)

func MetaFile(f *FileMetadataDO) *meta.File {
	if f == nil {
		return nil
	}

	return &meta.File{
		Id:         f.FId,
		Biz:        f.Biz,
		Name:       f.Name,
		Md5:        f.Md5,
		UserId:     f.UserId,
		Domain:     f.Domain,
		Size:       f.Size,
		ChunkSize:  f.ChunkSize,
		UploadDate: f.UploadTime,
		Type:       meta.EntityType(f.EntityType),
		ExtAttr:    f.ExtAttr,
	}
}

func DraFile(m *meta.File) *FileMetadataDO {
	if m == nil {
		return nil
	}

	return &FileMetadataDO{
		FId:        m.Id,
		Biz:        m.Biz,
		Name:       m.Name,
		Md5:        m.Md5,
		UserId:     m.UserId,
		Domain:     m.Domain,
		Size:       m.Size,
		ChunkSize:  m.ChunkSize,
		UploadTime: m.UploadDate,
		EntityType: meta.EntityType(m.Type),
		ExtAttr:    m.ExtAttr,
		RefCount:   1,
	}
}

func Map2Json(mp map[string]string) string {
	if mp == nil {
		return ""
	}
	jstr, _ := json.Marshal(mp)

	return string(jstr)
}
