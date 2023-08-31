#!/usr/bin/env bash

op=${1:-install} # or upgrade

cd "$(dirname "$0")"
ver=$(cat ./VERSION)
ns=onesource

helm3 "${op}" onesource -n "${ns}" \
    --set namespace="${ns}" \
    --set image.tag="${ver}" \
    ./helm-chart
