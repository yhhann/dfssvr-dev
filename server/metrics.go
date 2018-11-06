package server

import (
	"time"

	"jingoal.com/dfs/instrument"
)

var (
	rRate = 0.0 // kbit/s
	wRate = 0.0 // kbit/s
)

func entry(serviceName string) {
	instrument.InProcess <- &instrument.Measurements{
		Name:  serviceName,
		Value: 1,
	}
}

func exit(serviceName string) {
	instrument.InProcess <- &instrument.Measurements{
		Name:  serviceName,
		Value: -1,
	}
}

func instrumentPutFile(size int64, rate int64, serviceName string, biz string) {
	instrument.FileSize <- &instrument.Measurements{
		Name:  serviceName,
		Biz:   biz,
		Value: float64(size),
	}
	instrument.TransferRate <- &instrument.Measurements{
		Name:  serviceName,
		Value: float64(rate),
	}
}

func instrumentGetFile(fileSize int64, rate int64, serviceName string, biz string) {
	instrument.FileSize <- &instrument.Measurements{
		Name:  serviceName,
		Biz:   biz,
		Value: float64(fileSize),
	}
	instrument.TransferRate <- &instrument.Measurements{
		Name:  serviceName,
		Value: float64(rate),
	}
}

func startRateCheckRoutine() {
	go func() {
		// refresh rate every 5 seconds.
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r, err := instrument.GetTransferRate("GetFile")
				if err == nil && r > 0 { // err != nil ignored
					rRate = r
				}

				w, err := instrument.GetTransferRate("PutFile")
				if err == nil && w > 0 { // err != nil ignored
					wRate = w
				}
			}
		}
	}()
}
