#!/usr/bin/env bash

export PYTHONPATH=.        # add current directory to package path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"  # get directory of this script
python ${DIR}/onesource/__init__.py --read /var/data/in --write /var/data/out --temp /var/data/temp --overwrite --delete
