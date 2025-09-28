#!/bin/bash

echo "Starting ${RAY_ADDRESS} cluster"
ray \
      start \
      --head \
      --disable-usage-stats \
      --node-manager-port 43403 \
      --object-manager-port 43404 \
      --runtime-env-agent-port 43405 \
      --dashboard-agent-grpc-port 43406 \
      --dashboard-agent-listen-port 43407 \
      --metrics-export-port 43408 \
      --worker-port-list=10000,10001,10002 \
      --verbose
