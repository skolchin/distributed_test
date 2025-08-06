#!/bin/bash
set -e
tool="redis"
vol="redis_data"
id="$(docker ps -a -q -f name=${tool})"
if [ ! -z "${id}" ]; then
    echo "Docker ${id} already exists for ${tool}"
    docker start $id > /dev/null
else
    echo "Creating new volume ${vol} for ${tool}"
    docker volume create $vol  > /dev/null

    echo "Starting new docker for ${tool}"
    docker run \
        --name $tool \
        -p 7963:6379 \
        -v $vol:/data \
        -d redis redis-server \
        --appendonly yes \
        --save 60 1 \
        --maxmemory 32mb \
        --maxmemory-policy allkeys-lru

    id="$(docker ps -a -q -f name=${tool})"
fi
echo "Docker ${id} (re-)started, to stop it run:"
echo -e "\033[1;33m>>>\033[0m docker stop ${id}"
