#!/bin/bash
set -e

while read LINE; do export "${LINE//[$'\t\r\n']}"; done < .env

tool="grafana"
vol="grafana_data"
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
        -p 3000:3000 \
        -u $UID \
        -v $vol:/var/lib/grafana \
        -v /tmp/ray:/tmp/ray \
        -v /tmp/ray/session_latest/metrics/grafana/grafana.ini:/etc/grafana/grafana.ini \
        -e GF_SECURITY_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD \
        -e GF_PATHS_PROVISIONING=/tmp/ray/session_latest/metrics/grafana/provisioning \
        -d \
        grafana/grafana

    id="$(docker ps -a -q -f name=${tool})"
fi
echo "Docker ${id} (re-)started, to stop it run:"
echo -e "\033[1;33m>>>\033[0m docker stop ${id}"
