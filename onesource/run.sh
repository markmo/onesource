#!/usr/bin/env bash

source activate onesource  # activate conda environment for dependencies
export PYTHONPATH=.:/Users/d777710/src/DeepLearning/emnlp2017-relation-extraction/relation_extraction        # add current directory to package path
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd )"  # get directory of this script
python ${DIR}/__init__.py --read $1 --write $2 --temp $3 --overwrite --delete
