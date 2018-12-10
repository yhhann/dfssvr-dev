#!/bin/sh

export WORK_HOME=/ws
export GOROOT=$WORK_HOME/app/go
export GOPATH=$WORK_HOME/goapp
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin:$WORK_HOME/app/bin

echo $PATH && cd $WORK_HOME/dfsws/dfs && /usr/bin/make dfs
exit
