#!/usr/bin/env pwsh
#
# Launch a Ray cluster inside Docker for vLLM inference.
#
# The script was translated from `start_docker.sh` source using DeepSeek. Use at your own risk.
#
# This script can start either a head node or a worker node, depending on the
# --head or worker flag provided.
#
# Usage:
# 1. Designate one machine as the head node and execute:
#    .\start_docker.ps1 --head ...
#
# 2. On every worker machine, execute:
#    .\start_docker.ps1 --worker <head_node_ip> ...
# 
# Any additional arguments might follow the base command.
#
# Keep each terminal session open. Closing a session stops the associated Ray
# node and thereby shuts down the entire cluster.
#
# To stop the cluster, use:
#       docker stop node-<random_suffix>
#
param(
    [Parameter(Mandatory=$false, Position=0)]
    [string]$NodeType,
    
    [Parameter(Mandatory=$false, Position=1)]
    [string]$HeadNodeAddress,
    
    [Parameter(Mandatory=$false, ValueFromRemainingArguments=$true)]
    [string[]]$AdditionalArgs
)

# Check for minimum number of required arguments
if (-not $NodeType) {
    Write-Host "Usage: $($MyInvocation.MyCommand.Name) head | [worker head_node_ip] [additional_args...]"
    exit 1
}

# Validate node type
if ($NodeType -ne "head" -and $NodeType -ne "worker") {
    Write-Host "Error: Node type must be head or worker"
    exit 1
}

# Obtain host IP address and interface name
$HostIP = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { 
    $_.IPAddress -ne "127.0.0.1" -and $_.IPAddress -notlike "169.254.*"
} | Select-Object -First 1).IPAddress

$HostIFName = (Get-NetIPAddress -IPAddress $HostIP).InterfaceAlias

# Check for HF cache presence
$PathToHFHome = "$HOME\.cache\huggingface"
if (-not (Test-Path $PathToHFHome)) {
    # Cache does not exist, switch to local directory
    $LocalHF = ".\\.models"
    New-Item -ItemType Directory -Path $LocalHF -Force | Out-Null
    Write-Host "Warning: HuggingFace cache directory $PathToHFHome was not found, $LocalHF will be used instead"
    $PathToHFHome = $LocalHF
}

# Determine cluster head address
if ($NodeType -eq "head") {
    $HeadNodeAddress = $HostIP
    Write-Host "Cluster IP address will be $HeadNodeAddress"
} else {
    if (-not $HeadNodeAddress) {
        Write-Host "Error: head node address must be specified if node type is worker"
        exit 1
    }
}

# Build docker arguments
$DockerArgs = @(
    "-e", "NCCL_DEBUG=trace"
    "-e", "VLLM_LOGGING_LEVEL=debug"
    "-e", "CUDA_LAUNCH_BLOCKING=1"
    "-e", "VLLM_TRACE_FUNCTION=0"
    "-e", "NCCL_P2P_DISABLE=0"
    "-e", "OMP_NUM_THREADS=2"
    "-e", "GLOO_SOCKET_IFNAME=$HostIFName"
    "-e", "NCCL_SOCKET_IFNAME=$HostIFName"
    "-e", "RAY_DEDUP_LOGS=0"
)

if ($AdditionalArgs) {
    $DockerArgs += $AdditionalArgs
}

# Generate container name
if ($NodeType -eq "head") {
    $ContainerName = "ray-head"
} else {
    $RandomSuffix = Get-Random -Minimum 1000 -Maximum 9999
    $ContainerName = "ray-worker-$RandomSuffix"
}

# Build the Ray start command
$RayStartCmd = "ray start --block"
if ($NodeType -eq "head") {
    $RayStartCmd += " --head --port=6379 --disable-usage-stats"
} else {
    $RayStartCmd += " --address=${HeadNodeAddress}:6379"
}

# Determine image to start with and build it
if ($NodeType -eq "head") {
    $ImageName = "vllm-server"
    $Dockerfile = "./docker/Dockerfile.vllm-server"
} else {
    $ImageName = "vllm-client"
    $Dockerfile = "./docker/Dockerfile.vllm-client"
}

Write-Host "Building docker image $ImageName"
docker build -f $Dockerfile -t $ImageName:latest .

# Define cleanup function
function Cleanup {
    docker stop $ContainerName 2>$null
    docker rm $ContainerName 2>$null
}

# Register cleanup on script exit
trap { Cleanup } EXIT

# Launch the docker container
Write-Host "Host IP address: $HostIP on $HostIFName"
Write-Host "Docker $ContainerName arguments: $($DockerArgs -join ' ')"
Write-Host "Ray start command: $RayStartCmd"

$DockerCommand = @(
    "run"
    "--entrypoint", "/bin/bash"
    "--network", "host"
    "--name", $ContainerName
    "--shm-size", "10.24g"
    "--gpus", "all"
    "-v", "${PathToHFHome}:/root/.cache/huggingface"
    "-e", "VLLM_HOST_IP=$HostIP"
    $DockerArgs
    $ImageName
    "-c", $RayStartCmd
)

docker @DockerCommand
