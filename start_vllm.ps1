#!/usr/bin/env pwsh
#
# Launch vLLM serve with an LLM model
# Must run on a cluster started by ./start_vllm_docker.ps1

# The script was translated from `start_docker.sh` source using DeepSeek. Use at your own risk.

param(
    [Parameter(Mandatory=$true, Position=0)]
    [string]$Model,
    
    [Parameter(Mandatory=$false, Position=1)]
    [string]$HuggingfaceApiKey,
    
    [Parameter(Mandatory=$false, ValueFromRemainingArguments=$true)]
    [string[]]$AdditionalArgs
)

# Check if model is provided
if (-not $Model) {
    Write-Host "Model name is required"
    exit 1
}

# Check if API key looks like a HuggingFace token
if ($HuggingfaceApiKey -and $HuggingfaceApiKey -like "hf_*") {
    $HF_API_KEY = $HuggingfaceApiKey
}

# Check the docker is running
$DockerId = docker ps -f "name=ray-head" -q
if (-not $DockerId) {
    Write-Host "Cannot find Ray cluster head Docker container. Use ./start_vllm_docker.ps1 to run it"
    exit 1
}

# Find out how many GPUs are connected
$NumGPU = docker exec $DockerId bash -c "ray status | grep 'GPU' | tr '/.' ' ' | cut -d' ' -f4"
if (-not $NumGPU -or [int]$NumGPU -eq 0) {
    Write-Host "Cannot find any GPUs attached to the cluster. Ray status is:"
    docker exec $DockerId ray status
    exit 1
}

# Download the model
docker cp .\docker\download_model.sh ${DockerId}:/tmp 2>$null
if ($HF_API_KEY) {
    docker exec -it $DockerId bash /tmp/download_model.sh $Model $HF_API_KEY
} else {
    docker exec -it $DockerId bash /tmp/download_model.sh $Model
}

# Find out package versions
$Versions = docker exec $DockerId python3 -c "import ray, vllm; print(ray.__version__, '/', vllm.__version__)"

# Build vllm arguments
$VLLMArgs = @(
    "--gpu-memory-utilization", "0.9"
    "--port", "8080"
    "--pipeline-parallel-size", $NumGPU
    "--enable-prefix-caching"
)

if ($AdditionalArgs) {
    $VLLMArgs += $AdditionalArgs
}

# Launch the vLLM inside the docker
Write-Host "Docker container: $DockerId"
Write-Host "Number of GPUs detected: $NumGPU"
Write-Host "Ray / vLLM versions: $Versions"
Write-Host "Additional arguments: $($VLLMArgs -join ' ')"

docker exec -it $DockerId vllm serve $Model $VLLMArgs