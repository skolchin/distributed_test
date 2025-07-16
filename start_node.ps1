param (
    [string]$Address,
    [string]$NodeAddress = $Address  # Defaults to $Address if not provided
)

if (-not $Address) {
    Write-Output "Starting node locally"
    ray start `
        --num-cpus 1 `
        --resources '{"custom-resource": 1}' `
        --node-manager-port 43403 `
        --verbose
}
else {
    Write-Output "Starting node with cluster at $Address (node address is $NodeAddress)"

    ray start `
        --address "${Address}:6379" `
        --num-cpus 1 `
        --resources '{\"custom-resource\": 1}' `
        --node-manager-port 44403 `
        --object-manager-port 44404 `
        --runtime-env-agent-port 44405 `
        --dashboard-agent-grpc-port 44406 `
        --dashboard-agent-listen-port 44407 `
        --metrics-export-port 44408 `
        --verbose
}

Write-Output ""
Write-Output "Use 'ray stop' to stop the node"
