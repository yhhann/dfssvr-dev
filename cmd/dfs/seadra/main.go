package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/golang/glog"
	"gopkg.in/mgo.v2/bson"

	seadra "jingoal.com/dfs/cassandra"
)

const (
	GameOver uint8 = 100
)

var (
	serverStr  = flag.String("cql-servers", "127.0.0.1", "Seeds of cassandra cluster.")
	cqlUser    = flag.String("cql-user", "", "username")
	cqlPass    = flag.String("cql-pass", "", "password")
	quorum     = flag.Bool("quorum", false, "quorum")
	keyspace   = flag.String("keyspace", "dfs", "keyspace name of dfs.")
	conns      = flag.Int("conns", 3, "connections of one host.")
	timeout    = flag.Duration("timeout", time.Second, "timeout of cassandra.")
	fileCount  = flag.Int("file-count", 2, "count of file records.")
	routineCnt = flag.Int("routines", 100, "number of routine.")
	fileSize   = flag.Int("file-size", 4096, "size of file.")
	remove     = flag.Bool("remove", false, "remove files.")
)

var (
	op *seadra.MetaOp

	cmdChan    chan *Cmd
	resultChan chan *Cmd

	dump []byte
)

func main1() {
	flag.Parse()

	consistency := gocql.One
	if *quorum {
		consistency = gocql.Quorum
	}

	dump = make([]byte, *fileSize)
	rand.Seed(time.Now().Unix())

	op := seadra.NewMetaOp(*serverStr, *cqlUser, *cqlPass, *keyspace, consistency, *timeout, *conns)

	var wg sync.WaitGroup
	wg.Add(1)

	cmdChan = make(chan *Cmd, *routineCnt**fileCount)
	resultChan = make(chan *Cmd, *routineCnt**fileCount)

	start := time.Now()
	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < *fileCount; i++ {
			cmdChan <- &Cmd{}
		}
	}()

	count := 0
	go func() {
		for result := range resultChan {
			result.Status++
			if result.Status < 3 {
				cmdChan <- result
			} else {
				count++
				glog.Infof("game over count %d.", count)
			}
			if count >= *fileCount {
				wg.Done()
				break
			}
		}
	}()

	multiTaskRun(op, *routineCnt)

	wg.Wait()

	prefix := "Create/Lookup/Remove"
	if !*remove {
		prefix = "Create/Lookup"
	}

	glog.Errorf("%s %d files elapse %v.", prefix, count, time.Since(start))
	PrintFlags()
}

func PrintFlags() {
	glog.Errorf("flags:\n")
	flag.VisitAll(func(f *flag.Flag) {
		glog.Errorf("\t%s->%s\n", f.Name, f.Value)
	})
	glog.Errorln("------------------")
}

type Cmd struct {
	Status uint8
	Idmd5  string
}

func multiTaskRun(op *seadra.MetaOp, routineCnt int) {
	for i := 0; i < routineCnt; i++ {
		go func() {
			for cmd := range cmdChan {
				switch cmd.Status {
				case 0:
					// create
					dump[rand.Intn(*fileSize)] = byte(rand.Intn(127))
					idmd5, err := createFile(op, dump)
					if err != nil {
						glog.Warningf("Create file error, %v", err)
						cmd.Status = GameOver
					} else {
						cmd.Idmd5 = idmd5
						glog.Infof("Succeeded to create file %s ", idmd5)
						glog.Flush()
					}
					resultChan <- cmd

				case 1:
					// lookup
					if err := lookupFile(op, cmd.Idmd5); err != nil {
						glog.Warningf("Lookup file error, %v", err)
					}
					resultChan <- cmd

				case 2:
					// remove
					if *remove {
						if err := removeFile(op, cmd.Idmd5); err != nil {
							glog.Warningf("Remove file error, %v", err)
						}
					}
					cmd.Status = GameOver
					resultChan <- cmd

				default:
					glog.Infof("status %d.", cmd.Status)
					resultChan <- cmd
				}
			}
		}()
	}
}

func lookupFile(op *seadra.MetaOp, idmd5 string) error {
	ss := strings.Split(idmd5, "|")
	if len(ss) < 2 {
		return fmt.Errorf("id error, %s", idmd5)
	}
	fid, md5 := ss[0], ss[1]

	f1, err := op.LookupFileById(fid)
	if err != nil {
		return err
	}

	f2, err := op.LookupFileByMd5(md5, 2)
	if err != nil {
		return err
	}

	if f1.Id != f2.Id || f1.Md5 != f2.Md5 {
		return fmt.Errorf("not the same file. by id %s %s, by md5 %s %s", f1.Id, f1.Md5, f2.Id, f2.Md5)
	}

	return nil
}

func createFile(op *seadra.MetaOp, dump []byte) (string, error) {
	digest := md5.New()
	digest.Write(dump)
	md5Str := hex.EncodeToString(digest.Sum(nil))

	f := &seadra.File{
		Id:         bson.NewObjectId().Hex(),
		Biz:        "seadra-benchmark",
		ChunkSize:  1048576,
		Domain:     2,
		Name:       fmt.Sprintf("%d", time.Now().Unix()),
		Size:       int64(len(dump)),
		Md5:        md5Str,
		UploadDate: time.Now(),
		UserId:     "1001",
		Type:       seadra.EntitySeadraFS,
	}
	f.Metadata = map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	if err := op.SaveFile(f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s|%s", f.Id, f.Md5), nil
}

func removeFile(op *seadra.MetaOp, idmd5 string) error {
	ss := strings.Split(idmd5, "|")
	if len(ss) < 2 {
		return fmt.Errorf("id error, %s", ss)
	}
	fid := ss[0]
	op.RemoveFile(fid)

	return nil
}
