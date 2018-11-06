#!/bin/sh

/dfs/bin/dfssvr -server-name "$SERVER_ID" \
    -listen-addr "$LISTEN_ADDR" -register-addr "$REGISTER_ADDR" \
    -shard-dburi "$SHARD_URI" -shard-name "$SHARD_DBNAME" \
    -event-dburi "$EVENT_DBURI" -event-dbname "$EVENT_DBNAME" \
    -slog-dburi "$SLOG_DBURI" -slog-dbname "$SLOG_DBNAME" \
    -event-dbname "$EVENT_DBNAME" \
    -zk-addr "$ZK_ADDR" \
    -health-check-interval "$HEALTH_CHECK_INTERVAL" \
    -health-check-timeout "$HEALTH_CHECK_TIMEOUT" \
    -metrics-address "$METRICS_ADDR" \
    -metrics-path "$METRICS_PATH" \
    -gluster-log-dir "$LOG_DIR" \
    -log_dir "$LOG_DIR" -v 2 -logtostderr=false &

wait
