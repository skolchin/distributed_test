#!/bin/bash
set -e

/bin/bash ./download_model.sh $MODEL

HOST_IP=$(hostname -I | cut -d' ' -f1)
HOST_IFNAME=$(n=$(ifconfig | grep -n "${HOST_IP}" | cut -d: -f1); ifconfig | sed -n "$((n-1))p" | cut -d: -f1)

echo "Starting VLLM on ${HOST_IP}:${HOST_IFNAME}"

export VLLM_HOST_IP=${HOST_IP}
export GLOO_SOCKET_IFNAME=${HOST_IFNAME}
export NCCL_SOCKET_IFNAME=${HOST_IFNAME}

# echo "Environment: $(env)"

vllm serve $MODEL \
    --gpu-memory-utilization 0.9 \
    --port 8080 \
    --enable-prefix-caching \
    --distributed-executor-backend ray \
    --pipeline-parallel-size 1

# tail -f /dev/null