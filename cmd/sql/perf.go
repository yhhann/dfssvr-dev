package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/mgo.v2/bson"

	"jingoal.com/dfs/meta"
	"jingoal.com/dfs/sql"
)

var (
	lg            = log.New(os.Stdout, "[info]", log.Lshortfile)
	lgFlag        = flag.Bool("logflag", false, "if print log")
	metricsAddr   = flag.String("metrics-address", ":7070", "The address to listen on for metrics.")
	metricsPath   = flag.String("metrics-path", "/sql-metrics", "The path of metrics.")
	dsn           = flag.String("dsn", "root:zcl@tcp(127.0.0.1:3306)/file?charset=utf8", "datasource service name")
	channelSize   = flag.Int("channelsize", 10, "channel buffer size")
	taskSize      = flag.Int("tasksize", 100, "the number of task")
	goroutineSize = flag.Int("goroutinesize", 10, "the number of goroutine")

	isInfinite = false
	fmop       meta.FileMetaOp

	saveGauge           prometheus.Gauge
	findGauge           prometheus.Gauge
	findByMd5Gauge      prometheus.Gauge
	duplicateGauge      prometheus.Gauge
	findByDIdGauge      prometheus.Gauge
	deleteByDIdGauge    prometheus.Gauge
	deleteByFIdGauge    prometheus.Gauge
	operateErrHistogram *prometheus.HistogramVec

	done = make(chan bool)
)

func init() {
	rand.Seed(time.Now().Unix())

	saveGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "save_time",
			Help:      "Save file metadata duration.",
		},
	)

	findGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "find_time",
			Help:      "Find file metadata duration.",
		},
	)

	findByMd5Gauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "findbymd5_time",
			Help:      "Find file metadata by md5 and momain duration.",
		},
	)

	duplicateGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "duplicate_time",
			Help:      "Duplicate file metadata duration.",
		},
	)

	findByDIdGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "findbydid_time",
			Help:      "Find file metadata by did duration.",
		},
	)

	deleteByDIdGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "deletebydid_time",
			Help:      "Delete duplicate info by did duration.",
		},
	)

	deleteByFIdGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "deletebyfid_time",
			Help:      "Delete file metadata by fid duration.",
		},
	)

	operateErrHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dfs2_0",
			Subsystem: "perf",
			Name:      "operate_error",
			Help:      "Recored error count.",
			Buckets:   prometheus.ExponentialBuckets(0.1, 10, 7),
		},
		[]string{"service"},
	)

	prometheus.MustRegister(saveGauge)
	prometheus.MustRegister(findGauge)
	prometheus.MustRegister(findByMd5Gauge)
	prometheus.MustRegister(duplicateGauge)
	prometheus.MustRegister(findByDIdGauge)
	prometheus.MustRegister(deleteByDIdGauge)
	prometheus.MustRegister(deleteByFIdGauge)
	prometheus.MustRegister(operateErrHistogram)
}

type FMTask struct {
	op meta.FileMetaOp
}

func newFile() *meta.File {
	return &meta.File{
		Id:         bson.NewObjectId().Hex(),
		Biz:        "TEST-PERFORM",
		Name:       "我叫测试.txt",
		Md5:        bson.NewObjectId().Hex() + strconv.FormatInt(rand.Int63(), 10),
		UserId:     "jg",
		Domain:     119,
		Size:       rand.Int63n(2048),
		ChunkSize:  1024,
		UploadDate: time.Now(),
		Type:       meta.EntityGridFS,
		ExtAttr:    map[string]string{"color": "red", "isclassified": "true"},
	}
}

func (task FMTask) process() {

	if !isInfinite {
		defer func() {
			done <- true
		}()
	}

	fm := newFile()
	fId := fm.Id
	md5 := fm.Md5
	domain := fm.Domain

	stimer := prometheus.NewTimer(prometheus.ObserverFunc(saveGauge.Set))
	serr := task.op.Save(fm)
	stimer.ObserveDuration()
	if serr != nil {
		operateErrHistogram.WithLabelValues("save_err").Observe(1)
		return
	}

	if *lgFlag {
		lg.Println(" Save ")
	}

	ftimer := prometheus.NewTimer(prometheus.ObserverFunc(findGauge.Set))
	_, ferr := task.op.Find(fId)
	ftimer.ObserveDuration()
	if ferr != nil {
		operateErrHistogram.WithLabelValues("find_err").Observe(1)
		return
	}
	if *lgFlag {
		lg.Println(" Find ")
	}

	mtimer := prometheus.NewTimer(prometheus.ObserverFunc(findByMd5Gauge.Set))
	_, fberr := task.op.FindByMd5(md5, domain)
	mtimer.ObserveDuration()
	if fberr != nil {
		operateErrHistogram.WithLabelValues("findbymd5_err").Observe(1)
		return
	}
	if *lgFlag {
		lg.Println(" FindByMd5 ")
	}

	dtimer := prometheus.NewTimer(prometheus.ObserverFunc(duplicateGauge.Set))
	dId, derr := task.op.DuplicateWithId(fId, "", time.Now())
	dtimer.ObserveDuration()
	if derr != nil || dId == "" {
		operateErrHistogram.WithLabelValues("duplicate_err").Observe(1)
		return
	}
	if *lgFlag {
		lg.Println(" DuplicateWithId ")
	}

	fdtimer := prometheus.NewTimer(prometheus.ObserverFunc(findByDIdGauge.Set))
	_, fderr := task.op.Find(dId)
	fdtimer.ObserveDuration()
	if fderr != nil {
		operateErrHistogram.WithLabelValues("findbydid_err").Observe(1)
		return
	}
	if *lgFlag {
		lg.Println(" Find by dupId ")
	}

	ddtimer := prometheus.NewTimer(prometheus.ObserverFunc(deleteByDIdGauge.Set))
	toBeDel, _, derr := task.op.Delete(dId)
	ddtimer.ObserveDuration()
	if derr != nil {
		operateErrHistogram.WithLabelValues("delbydid_err").Observe(1)
		return
	}

	if *lgFlag {
		lg.Println(" Delete by dupId , result is ", toBeDel)
	}

	dftimer := prometheus.NewTimer(prometheus.ObserverFunc(deleteByFIdGauge.Set))
	toBeDelete, _, errd := task.op.Delete(fId)
	dftimer.ObserveDuration()
	if errd != nil {
		operateErrHistogram.WithLabelValues("delbyfid_err").Observe(1)
		return
	}

	if *lgFlag {
		lg.Println(" Delete by fileId ", toBeDelete)
	}

}

func NewFMTask() *FMTask {
	return &FMTask{
		op: fmop,
	}
}

type FMTaskProducer struct {
}

func (producer FMTaskProducer) DoProduct(taskChannel chan<- *FMTask, taskSize int) {
	lg.Println(" #####################[starting]########################### ")
	for i := 0; i < taskSize || taskSize <= 0; i++ {
		fmTask := NewFMTask()
		taskChannel <- fmTask
	}
}

type FMTaskConsumer struct {
}

func (consumer FMTaskConsumer) DoConsume(taskChannel <-chan *FMTask, goroutineSize int) {
	for i := 0; i < goroutineSize; i++ {
		go func() {
			for {
				task := <-taskChannel
				if task != nil {
					task.process()
				}
			}
		}()
	}
}

func StartMetrics() {
	go func() {
		http.Handle(*metricsPath, prometheus.UninstrumentedHandler())
		http.ListenAndServe(*metricsAddr, nil)
	}()
}

func main() {

	flag.Parse()

	fmt.Println(*dsn, *channelSize, *taskSize, *goroutineSize)

	checkFlags()

	StartMetrics()

	runtime.GOMAXPROCS(runtime.NumCPU())

	fmOperator, err := sql.NewFMOperator(*dsn)
	if err != nil {
		fmt.Printf("occur err ", err)
		os.Exit(1)
	}
	fmop = sql.NewMetaImpl(fmOperator)

	taskChanl := make(chan *FMTask, *channelSize)

	consumer := new(FMTaskConsumer)
	consumer.DoConsume(taskChanl, *goroutineSize)

	go func() {
		producer := new(FMTaskProducer)
		producer.DoProduct(taskChanl, *taskSize)
	}()

	if *taskSize <= 0 {
		isInfinite = true
		select {}
	}

	for i := 0; i < *taskSize; i++ {
		<-done
	}

	close(taskChanl)
	lg.Println("***************[ended]******************")

}

func checkFlags() {
	if *metricsAddr == "" {
		glog.Exit("Error: flag --metrics-address is required.")
	}
	if *metricsPath == "" {
		glog.Exit("Flag --metrics-path is required.")
	}
	if *dsn == "" {
		glog.Exit("Flag --dsn is required.")
	}

	if *channelSize < 0 {
		glog.Exit("Flag --channelSize is required.")
	}
	if *taskSize < 0 {
		glog.Exit("Flag --taskSize is required.")
	}
	if *goroutineSize < 0 {
		glog.Exit("Flag --goroutineSize is required.")
	}

}
