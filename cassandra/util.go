package cassandra

import "jingoal.com/dfs/meta"

func MetaFile(f *File) *meta.File {
	if f == nil {
		return nil
	}

	return &meta.File{
		Id:         f.Id,
		Biz:        f.Biz,
		Name:       f.Name,
		Md5:        f.Md5,
		UserId:     f.UserId,
		Domain:     f.Domain,
		Size:       f.Size,
		ChunkSize:  f.ChunkSize,
		UploadDate: f.UploadDate,
		Type:       meta.EntityType(f.Type),
		ExtAttr:    f.Metadata,
	}
}

func DraFile(m *meta.File) *File {
	if m == nil {
		return nil
	}

	return &File{
		Id:         m.Id,
		Biz:        m.Biz,
		Name:       m.Name,
		Md5:        m.Md5,
		UserId:     m.UserId,
		Domain:     m.Domain,
		Size:       m.Size,
		ChunkSize:  m.ChunkSize,
		UploadDate: m.UploadDate,
		Type:       EntityType(m.Type),
		Metadata:   m.ExtAttr,
	}
}
