#!/bin/bash
if [ $# -ne 1 ]; then
    ray start --num-cpus=1 --resources='{"custom-resource": 1}' --node-manager-port 43403
else
    ray start --address=${1}:6379 --num-cpus=1 --resources='{"custom-resource": 1}' --node-manager-port 43403
fi

echo ""
echo "Use 'ray stop' to stop the node"
