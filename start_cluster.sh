#!/bin/bash
ray start \
    --num-cpus 1 \
    --node-ip-address=95.31.13.220 \
    --node-manager-port 43403 \
    --dashboard-host 0.0.0.0 \
    --head \
    --verbose