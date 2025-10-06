#!/bin/bash

# Set environment variables
HOST_IP=$(hostname -I | cut -d' ' -f1)
HOST_IFNAME=$(n=$(ifconfig | grep -n "${HOST_IP}" | cut -d: -f1); ifconfig | sed -n "$((n-1))p" | cut -d: -f1)

export VLLM_HOST_IP=$HOST_IP
export GLOO_SOCKET_IFNAME=${HOST_IFNAME}
export NCCL_SOCKET_IFNAME=${HOST_IFNAME}
export VLLM_DOCKER_BUILD_CONTEXT=true

# Start Ray node
echo "Starting node ${HOST_IP}:${HOST_IFNAME} with ${RAY_ADDRESS} cluster"
ray start --block --address=$RAY_ADDRESS --verbose

# ray start \
#     --block \
#     --address=$RAY_ADDRESS \
#     --node-manager-port 44403 \
#     --object-manager-port 44404 \
#     --runtime-env-agent-port 44405 \
#     --dashboard-agent-grpc-port 44406 \
#     --dashboard-agent-listen-port 44407 \
#     --metrics-export-port 44408 \
#     --verbose
