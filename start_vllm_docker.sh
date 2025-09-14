#!/bin/bash
#
# Launch a Ray cluster inside Docker for vLLM inference.
# Origin: https://github.com/vllm-project/vllm/blob/main/examples/online_serving/run_cluster.sh
#
# This script can start either a head node or a worker node, depending on the
# --head or --worker flag provided as the third positional argument.
#
# Usage:
# 1. Designate one machine as the head node and execute:
#    bash run_cluster.sh --head ...
#
# 2. On every worker machine, execute:
#    bash run_cluster.sh --worker <head_node_ip> ...
# 
# Any additional arguments might follow the base command.
#
# Keep each terminal session open. Closing a session stops the associated Ray
# node and thereby shuts down the entire cluster.
#
# The container is named "node-<random_suffix>". To open a shell inside
# a container after launch, use:
#       docker exec -it node-<random_suffix> /bin/bash
#
# Then, you can execute vLLM commands on the Ray cluster as if it were a
# single machine, e.g. vllm serve ...
#
# To stop the container, use:
#       docker stop node-<random_suffix>

# Check for minimum number of required arguments.
if [ $# -eq 0    ]; then
    echo "Usage: $0 --head|--worker [head_node_ip] [additional_args...]"
    exit 1
fi

# Extract the node type
NODE_TYPE="$1"
shift 1

if [ "${NODE_TYPE}" != "--head" ] && [ "${NODE_TYPE}" != "--worker" ]; then
    echo "Error: Node type must be --head or --worker"
    exit 1
fi

if [ "${NODE_TYPE}" == "--head" ]; then
    HEAD_NODE_ADDRESS=$(hostname -I | cut -d' ' -f1)
    echo "Cluster IP address will be ${HEAD_NODE_ADDRESS}"
else
    HEAD_NODE_ADDRESS="$1"
    shift 1

    if [[ -z $HEAD_NODE_ADDRESS ]]; then
        echo "Error: head node address must be specified if node type is --worker"
        exit 1
    fi
fi

# Check for HF cache presence
PATH_TO_HF_HOME="${HOME}/.cache/huggingface"
if [[ ! -d "$PATH_TO_HF_HOME" ]]; then
    # Cache does not exist, switch to local directory
    LOCAL_HF="./.models"
    mkdir -p "${LOCAL_HF}"
    echo "Warning: HuggingFace cache directory ${PATH_TO_HF_HOME} was not found, ${LOCAL_HF} will be used instead"
    PATH_TO_HF_HOME="${LOCAL_HF}"
fi

# Keep extra arguments for the docker
ADDITIONAL_ARGS=("$@")

# Generate a unique container name with random suffix for worker nodes
# For the head node, fixed name is used
if [ "${NODE_TYPE}" == "--head" ]; then
    CONTAINER_NAME="ray-head"
else
    CONTAINER_NAME="ray-worker-${RANDOM}"
fi

# Obtain a host IP address (must be provided to VLLM)
VLLM_NODE_ADDRESS=$(hostname -I | cut -d' ' -f1)

# Build the Ray start command based on the node role.
# The head node manages the cluster and accepts connections on port 6379, 
# while workers connect to the head's address.
RAY_START_CMD="ray start --block --disable-usage-stats"
if [ "${NODE_TYPE}" == "--head" ]; then
    RAY_START_CMD+=" --head --port=6379"
else
    RAY_START_CMD+=" --address=${HEAD_NODE_ADDRESS}:6379"
fi

# Define a cleanup routine that removes the container when the script exits.
# This prevents orphaned containers from accumulating if the script is interrupted.
cleanup() {
    docker stop "${CONTAINER_NAME}" >/dev/null 2>&1
    docker rm "${CONTAINER_NAME}" >/dev/null 2>&1
}
trap cleanup EXIT

# Build the VLLM command for head node only
# VLLM_SERVE_CMD=""
# if [ "${NODE_TYPE}" == "--head" ]; then
#     VLLM_SERVE_CMD="&& vllm serve Qwen/Qwen3-0.6B --port 8080 --gpu_memory_utilization 0.9 pipeline_parallel_size=1"

# Launch the docker
echo "Launching docker ${CONTAINER_NAME}, extra arguments: ${ADDITIONAL_ARGS[@]}"
echo "Ray start command: ${RAY_START_CMD}"
echo "VLLM IP address: ${VLLM_NODE_ADDRESS}"

docker run \
    --entrypoint /bin/bash \
    --network host \
    --name "${CONTAINER_NAME}" \
    --shm-size 10.24g \
    --gpus all \
    -v "${PATH_TO_HF_HOME}:/root/.cache/huggingface" \
    -e VLLM_HOST_IP=${VLLM_NODE_ADDRESS} \
    -e NCCL_DEBUG="TRACE" \
    -e VLLM_LOGGING_LEVEL="DEBUG" \
    -e CUDA_LAUNCH_BLOCKING="1" \
    -e VLLM_TRACE_FUNCTION="0" \
    -e NCCL_P2P_DISABLE="1" \
    -e OMP_NUM_THREADS="2" \
    "${ADDITIONAL_ARGS[@]}" \
    vllm/vllm-openai -c "${RAY_START_CMD}"
