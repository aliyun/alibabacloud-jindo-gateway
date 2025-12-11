#!/bin/bash

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$BIN_DIR")"

LOG_DIR=$JINDO_GATEWAY_LOG_DIR
if [ -z "$LOG_DIR" ]; then
  LOG_DIR="/tmp/jindo-gateway"
fi
if [ ! -d "$LOG_DIR" ]; then
  mkdir -p "$LOG_DIR"
fi
PID_DIR="$LOG_DIR/pid"
PID_FILE="$PID_DIR/datanode.pid"

if [ ! -f "$PID_FILE" ]; then
  echo "Error: PID file not found. Is JindoDataNode running?"
  exit 1
fi

PID=$(cat "$PID_FILE")

if ! kill -0 $PID > /dev/null 2>&1; then
  echo "Error: Process with PID $PID is not running or does not exist"
  rm -f "$PID_FILE"
  exit 1
fi

echo "Stopping JindoDataNode with PID: $PID"
kill $PID

TIMEOUT=30
COUNT=0
while kill -0 $PID > /dev/null 2>&1; do
  sleep 1
  COUNT=$((COUNT + 1))
  if [ $COUNT -ge $TIMEOUT ]; then
    echo "Timeout reached. Force killing JindoDataNode..."
    kill -9 $PID
    break
  fi
done

rm -f "$PID_FILE"
echo "JindoDataNode stopped successfully"