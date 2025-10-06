#!/bin/bash
set -e

# Start the head
echo "Starting cluster"
# ray \
#       start \
#       --head \
#       --disable-usage-stats \
#       --node-manager-port 43403 \
#       --object-manager-port 43404 \
#       --runtime-env-agent-port 43405 \
#       --dashboard-agent-grpc-port 43406 \
#       --dashboard-agent-listen-port 43407 \
#       --metrics-export-port 43408 \
#       --worker-port-list=10000,10001,10002 \
#       --verbose
ray start --head --disable-usage-stats --verbose

echo "Waiting for 30 seconds for nodes to join in"
sleep 30

echo "Starting VLLM"

# Find out how many GPUS are connected
NUM_GPU=$(ray status | grep "GPU" | tr '/.' ' ' | cut -d' ' -f4); 
if [[ -z "${NUM_GPU}" ]] || [[ NUM_GPU -eq 0 ]]; then
    echo "Cannot find any GPUs attached to the cluster. Ray status is:"
    ray status
    exit 1
fi
echo "${NUM_GPU} GPUs detected"

# Download model
/bin/bash ./download_model.sh $MODEL
test $?
echo "Using model ${MODEL}"

# Set environment variables
HOST_IP=$(hostname -I | cut -d' ' -f1)
HOST_IFNAME=$(n=$(ifconfig | grep -n "${HOST_IP}" | cut -d: -f1); ifconfig | sed -n "$((n-1))p" | cut -d: -f1)

echo "Starting VLLM on ${HOST_IP}:${HOST_IFNAME}"

export VLLM_HOST_IP=$HOST_IP
export GLOO_SOCKET_IFNAME=${HOST_IFNAME}
export NCCL_SOCKET_IFNAME=${HOST_IFNAME}
export VLLM_DOCKER_BUILD_CONTEXT=true

# Start
vllm serve $MODEL \
    --gpu-memory-utilization 0.9 \
    --port 8080 \
    --enable-prefix-caching \
    --distributed-executor-backend ray \
    --pipeline-parallel-size $NUM_GPU
