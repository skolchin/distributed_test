#!/bin/bash
rm -f nohup.out

if [ $# -ne 1 ]; then
    echo "Starting cluster locally"

    ray start \
        --num-cpus 1 \
        --head \
        --verbose

else
    echo "Starting cluster at ${1}"

    ray start \
        --num-cpus 1 \
        --node-ip-address=${1} \
        --dashboard-host 0.0.0.0 \
        --head \
        --verbose

fi

echo ""
echo "Use 'ray stop' to stop the cluster"
