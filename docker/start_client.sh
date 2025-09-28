#!/bin/bash
echo "Starting node at ${RAY_ADDRESS} cluster"
ray start \
    --block \
    --address=$RAY_ADDRESS \
    --node-manager-port 44403 \
    --object-manager-port 44404 \
    --runtime-env-agent-port 44405 \
    --dashboard-agent-grpc-port 44406 \
    --dashboard-agent-listen-port 44407 \
    --metrics-export-port 44408 \
    --verbose
