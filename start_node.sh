#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Starting node locally"
    ray start \
        --num-cpus=1 \
        --resources='{"custom-resource": 1}' \
        --node-manager-port 43403 \
        --verbose
else
    echo "Starting node at ${1}"

    ray start \
        --address=${1}:6379 \
        --num-cpus=1 \
        --resources='{"custom-resource": 1}' \
        --node-manager-port 43403 \
        --object-manager-port 43404 \
        --runtime-env-agent-port 43405 \
        --dashboard-agent-grpc-port 43406 \
        --metrics-export-port 43407 \
        --verbose

fi

echo ""
echo "Use 'ray stop' to stop the node"
