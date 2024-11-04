#!/bin/bash


export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}


export EXECUTION_DATE=$1 # e.g., 20241029
export POSTGRES_JAR=postgresql-42.7.4.jar
export POSTGRES_JAR_PATH=${PROJECT_ROOT_PATH}/lib/${POSTGRES_JAR}

# Hardcoded credentials just to simplify the challenge.
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="sf_fire"
export DB_USER="sf_fire"
export DB_PWD="prMSohYm0GoY9bnIZvEAnE5Go"


pip install -r requirements.txt


spark-submit \
    --driver-class-path ${POSTGRES_JAR_PATH} \
    --jars ${POSTGRES_JAR} \
    --master local[*] \
    ./src/spark.py
