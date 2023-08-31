#!/usr/bin/env bash

cd "$(dirname "$0")"
ver=$(cat ./VERSION)
cr="$CR"

docker build . -t "${cr}/onesource:${ver}" -f dockerfile
docker push "${cr}/onesource:${ver}"

cd -