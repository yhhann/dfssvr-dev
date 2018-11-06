package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"path/filepath"
	"testing"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
)

func TestGlusterFs(t *testing.T) {
	shard := &metadata.Shard{
		Name:        "gluster-test",
		Uri:         "mongodb://192.168.55.193:27017/",
		PathVersion: 3,
		PathDigit:   2,
		VolHost:     "192.168.55.193",
		VolName:     "vol2",
		VolBase:     "base-test",
	}
	// Initialize
	handler, err := NewGlusterHandler(shard, filepath.Join("/var/log/dfs", shard.Name))
	if err != nil {
		t.Errorf("NewGlusterHandler error %v", err)
		return
	}

	// File info
	info := transfer.FileInfo{Name: "mytestfile",
		Domain: 2,
		User:   101,
	}

	// Create file
	file, err := handler.Create(&info)
	if err != nil {
		t.Errorf("Create file error %v", err)
	}

	p, degist := makePayload(2049)
	wl, err := file.Write(p)
	if err != nil {
		t.Errorf("Write error %v\n", err)
	}
	if wl != 2049 {
		t.Errorf("Write error %v\n", err)
	}

	f, ok := file.(*GlusterFile)
	if !ok {
		t.Errorf("create file type is not gluster file\n")
	}

	fid := f.info.Id
	if err := file.Close(); err != nil {
		t.Errorf("Close write file error %v\n", err)
	}

	// Open file
	nf, err := handler.Open(fid, 2)
	if err != nil {
		t.Errorf("Open file error %v\n", err)
	}

	nf1, ok := nf.(*GlusterFile)
	if !ok {
		t.Errorf("open file type is not gluster file\n")
	}

	// Compare md5
	if nf1.info.Md5 != degist {
		t.Errorf("write read file, md5 not equals\n")
	}

	if err := nf.Close(); err != nil {
		t.Errorf("Close read file error %v\n", err)
	}

	if _, _, err := handler.Remove(fid, 2); err != nil {
		t.Errorf("Remove file error %v\n", err)
	}
}

func makePayload(len int) ([]byte, string) {
	payload := make([]byte, len)
	payload[0] = 0x5a
	payload[len-1] = 0xa5
	wsum := md5.New()
	wsum.Write(payload)
	return payload, hex.EncodeToString(wsum.Sum(nil))
}
