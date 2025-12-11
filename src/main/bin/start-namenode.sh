#!/bin/bash

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$BIN_DIR")"
CONF_DIR="$PROJECT_DIR/etc/jindo-gateway"

LOG_DIR=$JINDO_GATEWAY_LOG_DIR
if [ -z "$LOG_DIR" ]; then
  LOG_DIR="/tmp/jindo-gateway"
fi
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi
LOG_FILE="namenode.log"
echo "NameNode log file: $LOG_DIR/$LOG_FILE"

PID_DIR="$LOG_DIR/pid"
if [ -f "$PID_DIR/namenode.pid" ]; then
  PID=$(cat "$PID_DIR/namenode.pid")
  if ps -p $PID > /dev/null 2>&1; then
    echo "Error: NameNode is already running with PID $PID"
    exit 1
  else
    echo "Warning: Removing stale PID file $PID_DIR/namenode.pid"
    rm -f "$PID_DIR/namenode.pid"
  fi
fi

echo "Checking Java environment..."
if ! command -v java &> /dev/null; then
  echo "Error: Java is not installed or not in PATH"
  exit 1
fi

CLASSPATH="$PROJECT_DIR/share/jindo-gateway-1.0-SNAPSHOT/jindo-gateway-1.0-SNAPSHOT.jar:$CONF_DIR"

if [ -d "$PROJECT_DIR/lib" ]; then
  for jar in "$PROJECT_DIR"/lib/jindo-gateway/*.jar "$PROJECT_DIR"/lib/hadoop/*.jar "$PROJECT_DIR"/lib/common/*.jar; do
    if [ -f "$jar" ]; then
      CLASSPATH="$CLASSPATH:$jar"
    fi
  done
fi

JAVA_OPTS="-Djava.net.preferIPv4Stack=true -Djindo.home.dir=$PROJECT_DIR -Dlog.path=$LOG_DIR/$LOG_FILE -Dlog4j.configuration=file:$CONF_DIR/log4j.properties"

echo "Starting JindoNameNode..."
echo "Using classpath: $CLASSPATH"

PID_DIR="$LOG_DIR/pid"
mkdir -p "$PID_DIR"

java $JAVA_OPTS -cp "$CLASSPATH" com.aliyun.jindodata.gateway.hdfs.namenode.JindoNameNode "$@" > "$LOG_DIR/namenode.out" 2>&1 &
PID=$!
echo $PID > "$PID_DIR/namenode.pid"

echo "JindoNameNode started with PID: $PID"
echo "Logs are written to: $LOG_DIR/namenode.out"
echo "PID file is stored at: $PID_DIR/namenode.pid"