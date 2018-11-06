export GOPATH := ${GOPATH}:$(shell pwd)
export LDFLAG=" -s -X main.buildTime=`date -u '+%Y%m%d-%I%M%S%Z'`"

all: dfs
	@echo "make dfs          : build dfs"
	@echo "make tools        : build tools"
	@echo "make fmt          : run go fmt tool"
	@echo "make clean        : clean dfs binary"

dfs: fmt
	go install -ldflags ${LDFLAG} jingoal/dfs/cmd/dfs/dfscln
	go install -ldflags ${LDFLAG} jingoal/dfs/cmd/dfs/dfssvr

.PHONY: proto
proto:
	protoc --go_out=plugins=grpc:. proto/discovery/*.proto
	protoc --go_out=plugins=grpc:. proto/transfer/*.proto

java:
	protoc --plugin=protoc-gen-grpc-java=/usr/local/bin/protoc-gen-grpc-java --grpc-java_out=../dfs-client/src/main/java --java_out=../dfs-client/src/main/java src/jingoal/dfs/proto/discovery/*.proto
	protoc --plugin=protoc-gen-grpc-java=/usr/local/bin/protoc-gen-grpc-java --grpc-java_out=../dfs-client/src/main/java --java_out=../dfs-client/src/main/java src/jingoal/dfs/proto/transfer/*.proto

debug:
	go install -gcflags "-N -l" jingoal/dfs/cmd/dfs/dfscln
	go install -gcflags "-N -l" jingoal/dfs/cmd/dfs/dfssvr

fmt:
#	go fmt jingoal/dfs/cmd/dfs

clean:
	rm -fr bin
	rm -fr pkg
