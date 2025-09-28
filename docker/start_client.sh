#!/bin/bash
echo "Starting node of ${RAY_ADDRESS} cluster"
ray start --block --address="${RAY_ADDRESS}:6379"