package main

import (
	"flag"

	"github.com/golang/glog"
)

func main() {
	flag.Parse()

	glog.V(2).Infof("Main func.\n")
	main1()
}
