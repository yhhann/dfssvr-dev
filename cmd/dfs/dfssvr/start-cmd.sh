#! /bin/bash

###########################################################
#
# start-dfs-server.sh
#
# It is used to start DFS server.
#
###########################################################

set -e

DIR=$(cd `dirname $0`; pwd|awk -F'/bin' '{print $1}')
chmod +x $DIR/bin/*

outip=$(/sbin/ifconfig em2 2>/dev/null| grep "inet addr" | awk {'print $2'} | awk -F':' {'print $2'})
if [ -z $outip ];then
   outip=$(/sbin/ifconfig eth1 2>/dev/null | grep "inet addr" | awk {'print $2'} | awk -F':' {'print $2'})
fi

if [ -z $SERVER_ID ]; then
   SERVER_ID=$outip
fi

if [ -z $REGISTER_ADDR ]; then
   REGISTER_ADDR="$outip":10000
fi

if [ -z $LISTEN_ADDR ]; then
  LISTEN_ADDR=":10000"
fi

if [ -z $ZK_ADDR ]; then
  ZK_ADDR="192.168.0.49:2181,192.168.0.50:2181,192.168.0.55:2181"
fi

if [ -z $SHARD_URI ]; then
  SHARD_URI="mongodb://192.168.0.42:27020,192.168.0.43:27020,192.168.0.47:27020/?maxpoolsize=4096&connecttimeoutms=150000"
fi

if [ -z $SHARD_DBNAME ]; then
  SHARD_DBNAME=shard
fi

if [ -z $EVENT_DBNAME ]; then
  EVENT_DBNAME=eventdb
fi

if [ -z $SHIELD_TIMEOUT]; then
  SHIELD_TIMEOUT=60s
fi

if [ -z $HEALTH_CHECK_INTERVAL ]; then
  HEALTH_CHECK_INTERVAL=60
fi

if [ -z $HEALTH_CHECK_TIMEOUT ]; then
  HEALTH_CHECK_TIMEOUT=10
fi

if [ -z $METRICS_ADDR ]; then
  METRICS_ADDR=:2020
fi

if [ -z $METRICS_PATH ]; then
  METRICS_PATH=/dfs-metrics
fi

if [ -z $LOG_DIR ]; then
  LOG_DIR=$DIR/logs
fi

if [ ! -e $LOG_DIR ]; then
  mkdir -p $LOG_DIR
fi

if [ -z $LOG_LEVEL ]; then
  LOG_LEVEL=2
fi

STDOUT_FILE=$LOG_DIR/dfsserver_`date +%F_%T`.txt

set +e
PIDS=`pgrep ^dfssvr$`
if [ $? -eq 0 ]; then
  echo "ERROR: DFS Server already started!"
  echo "PID: $PIDS"
  exit 1
fi
set -e

nohup $DIR/bin/dfssvr -server-name=$SERVER_ID \
  -listen-addr=$LISTEN_ADDR -register-addr=$REGISTER_ADDR \
  -shard-dburi=$SHARD_URI -shard-name=$SHARD_DBNAME \
  -event-dbname=$EVENT_DBNAME \
  -slog-dbname=$SHARD_DBNAME \
  -zk-addr=$ZK_ADDR \
  -health-check-interval=$HEALTH_CHECK_INTERVAL \
  -health-check-timeout=$HEALTH_CHECK_TIMEOUT \
  -metrics-address=$METRICS_ADDR \
  -metrics-path=$METRICS_PATH \
  -gluster-log-dir=$LOG_DIR \
  -shield-timeout=$SHIELD_TIMEOUT \
  -log_dir=$LOG_DIR -v $LOG_LEVEL -logtostderr=false \
  > $STDOUT_FILE 2>&1 &

PIDS=`pgrep ^dfssvr$`
if [ $? -ne 0 ]; then
  echo "ERROR: The service DFS Server does not started!"
  exit 1
fi

echo "Service DFS Server started."
