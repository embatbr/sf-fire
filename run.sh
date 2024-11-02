#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


export EXECUTION_DATE=$1


pip install -r requirements.txt

# python ./src/spark.py
spark-submit \
    --master local[*] \
    ./src/spark.py
