#!/bin/bash
#
# Launch vLLM serve with an LLM model
# Must run on a cluster started by ./start_vllm_docker.sh

# Check for minimum number of required arguments
if [ $# -eq 0 ]; then
    echo "Usage: $0 model [huggingface_api_key] [additional_args...]"
    exit 1
fi

# Get the model name
MODEL="$1"
if [[ -z "${MODEL}" ]]; then
    echo "Model name is required"
    exit 1
fi
shift 1

# Check the API key is provided
if [[ "${1}" == "hf*" ]]; then
    HF_API_KEY="${1}"
    shift 1
fi

# Check the docker is running
DOCKER_ID=$(docker ps -f name="ray-head" -q)
if [[ -z "${DOCKER_ID}" ]]; then
    echo "Cannot find Ray cluster head Docker container. Use ./start_vllm_docker.sh to run it"
    exit 1
fi

# Find out how many GPUS are connected
NUM_GPU=$(docker exec $DOCKER_ID bash -c "ray status | grep "GPU" | tr '/.' ' ' | cut -d' ' -f4"); 
if [[ -z "${NUM_GPU}" ]] || [[ NUM_GPU -eq 0 ]]; then
    echo "Cannot find any GPUs attached to the cluster. Ray status is:"
    docker exec $DOCKER_ID ray status
    exit 1
fi

# Download the model
docker cp _download_model.sh $DOCKER_ID:/tmp > /dev/null 2>&1
docker exec -it $DOCKER_ID bash /tmp/_download_model.sh $MODEL $HF_API_KEY

# Find out package versions
VERSIONS=$(docker exec $DOCKER_ID python3 -c "import ray, vllm; print(ray.__version__, '/', vllm.__version__)")

# Build vllm arguments assuming some predefines
# This assumes that each node has only 1 GPU, more sophisticated logic need if is not so
ADDITIONAL_ARGS=(
    "--gpu-memory-utilization" "0.9"
    "--port" "8080"
    "--pipeline-parallel-size" "${NUM_GPU}"
)
ADDITIONAL_ARGS+=("$@")

# Launch the vLLM inside the docker
echo "Docker container: ${DOCKER_ID}"
echo "Number of GPUs detected: ${NUM_GPU}"
echo "Ray / vLLM versions: ${VERSIONS}"
echo "Additional arguments: ${ADDITIONAL_ARGS[@]}"

docker exec -it $DOCKER_ID vllm serve $MODEL "${ADDITIONAL_ARGS[@]}"
