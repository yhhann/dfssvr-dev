package fileop

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"jingoal.com/dfs/metadata"
	"jingoal.com/dfs/proto/transfer"
)

func TestGlustraReadAndWrite(t *testing.T) {
	shard := &metadata.Shard{
		ShdType:     metadata.Glustra,
		Name:        "glustra-test",
		Uri:         "127.0.0.1",
		PathVersion: 3,
		PathDigit:   2,
		VolHost:     "192.168.64.176",
		VolName:     "repl",
		VolBase:     "glustra1",
		Attr: map[string]interface{}{
			"keyspace":    "dfs",
			"consistency": "quorum",
			"timeout":     "600",
			"conns":       "2",
		},
	}
	// Initialize
	handler, err := NewGlustraHandler(shard, filepath.Join("/var/log/dfs", shard.Name))
	if err != nil {
		t.Errorf("NewGlustraHandler error %v", err)
		return
	}

	// File info
	info := transfer.FileInfo{
		Domain: 2,
		User:   101,
		Biz:    "glustra-file",
		Name:   time.Now().Format("2006-01-02 15:04:05"),
	}

	// Create file
	file, err := handler.Create(&info)
	if err != nil {
		t.Errorf("Create file error %v", err)
	}

	f, ok := file.(*GlustraFile)
	if !ok {
		t.Errorf("create file type is not gluster file\n")
	}

	// Write
	p, digest := makePayload(2049)
	wl, err := file.Write(p)
	if err != nil {
		t.Errorf("Write error %v\n", err)
	}
	if wl != 2049 {
		t.Errorf("Write error %v\n", err)
	}

	// update meta
	f.updateFileMeta(map[string]interface{}{
		BSMetaKey_WeedFid: "alaska",
	})

	// Close
	fid := f.info.Id
	if err := file.Close(); err != nil {
		t.Errorf("Close write file error %v\n", err)
	}

	// Open file with id
	nf, err := handler.Open(fid, 2)
	if err != nil {
		t.Errorf("Open file error %v\n", err)
	}

	// Compare md5
	if nf.GetFileInfo().Md5 != digest {
		t.Errorf("read file, md5 not equals\n")
	}

	if nf.getFileMeta().Fid != "alaska" {
		t.Errorf("read file, meta not equals\n")
	}

	result, err := readAndCompareMd5(nf, digest)
	if err != nil {
		t.Errorf("read file error %v", err)
	}
	if !result {
		t.Errorf("read file error, not the same md5.")
	}

	// Close file
	if err := nf.Close(); err != nil {
		t.Errorf("Close read file error %v\n", err)
	}

	// File file with md5
	fidByMd5, err := handler.FindByMd5(digest, 2, 2049)
	if err != nil {
		t.Errorf("Find by md5 error %v\n", err)
	}

	if fidByMd5 != fid {
		t.Errorf("Find by md5 error, not the same id.")
	}

	// duplicate
	did, err := handler.Duplicate(fid)
	if err != nil {
		t.Errorf("Duplicate error %v.", err)
	}

	// Find
	df, err := handler.Open(did, 2)
	result, err = readAndCompareMd5(df, digest)
	if err != nil {
		t.Errorf("read file error %v", err)
	}
	if !result {
		t.Errorf("read file error, not the same md5.")
	}

	// Close file
	if err := df.Close(); err != nil {
		t.Errorf("Close read file error %v\n", err)
	}

	if _, _, err := handler.Remove(fid, 2); err != nil {
		t.Errorf("Remove file error %v\n", err)
	}

	if _, _, err := handler.Remove(did, 2); err != nil {
		t.Errorf("Remove file error %v\n", err)
	}
}

func makePayload(len int) ([]byte, string) {
	payload := make([]byte, len)
	payload[rand.Intn(len)] = 0x5a
	wsum := md5.New()
	wsum.Write(payload)
	return payload, hex.EncodeToString(wsum.Sum(nil))
}

func readAndCompareMd5(nf io.Reader, digest string) (bool, error) {
	h := md5.New()
	p1 := make([]byte, 1024)
	for {
		n, err := nf.Read(p1)
		if err == io.EOF {
			_, err = h.Write(p1[:n])
			break
		}
		if err != nil {
			return false, err
		}
		_, err = h.Write(p1[:n])
	}

	// Compare md5 again
	if hex.EncodeToString(h.Sum(nil)) != digest {
		return false, errors.New("md5 not equals")
	}

	return true, nil
}
