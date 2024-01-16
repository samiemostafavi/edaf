#!/bin/sh

# run influxd
nohup sh -c influxd &

# wait a while
sleep 1

# initial influxdb setup
influx setup \
  --username edaf \
  --password 4c5f28e30698bf883e18193 \
  --org KTH \
  --bucket latency \
  --force

# read the authentication info including the token
influx auth list --json > influx_auth.json

exec "$@"