#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Starting node locally"
    ray start \
        --num-cpus=1 \
        --resources='{"custom-resource": 1}' \
        --node-manager-port 44403 \
        --verbose
else
    echo "Starting node with cluster at ${1}"

    ray start \
        --address=${1}:6379 \
        --resources='{"custom-resource": 1}' \
        --node-manager-port 44403 \
        --object-manager-port 44404 \
        --runtime-env-agent-port 44405 \
        --dashboard-agent-grpc-port 44406 \
        --dashboard-agent-listen-port 44407 \
        --metrics-export-port 44408 \
        --verbose

fi

echo ""
echo "Use 'ray stop' to stop the node"
