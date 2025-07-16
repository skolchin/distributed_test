param (
    [string]$Address
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
    Write-Output "Starting node at $Address"

    ray start `
        --address "${Address}:6379" `
        --num-cpus 1 `
        --resources '{\"custom-resource\": 1}' `
        --node-manager-port 43403 `
        --object-manager-port 43404 `
        --runtime-env-agent-port 43405 `
        --dashboard-agent-grpc-port 43406 `
        --metrics-export-port 43407 `
        --verbose
}

Write-Output ""
Write-Output "Use 'ray stop' to stop the node"
